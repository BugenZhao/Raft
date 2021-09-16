use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use crate::msg::*;
use crate::service::*;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
    next_ts: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        // Your code here.
        let ts = self
            .next_ts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        Ok(TimestampResponse { ts })
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

impl Value {
    fn as_ts(&self) -> u64 {
        match self {
            Value::Timestamp(ts) => *ts,
            Value::Vector(_) => panic!(),
        }
    }

    fn as_bytes(&self) -> &[u8] {
        match self {
            Value::Timestamp(_) => panic!(),
            Value::Vector(bytes) => bytes,
        }
    }
}

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    #[inline]
    fn column_ref(&self, column: Column) -> &BTreeMap<Key, Value> {
        match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        }
    }

    #[inline]
    fn column_mut(&mut self, column: Column) -> &mut BTreeMap<Key, Value> {
        match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        }
    }

    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: &[u8],
        column: Column,
        ts_range: impl RangeBounds<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.
        let column = self.column_ref(column);
        let key_start = match ts_range.start_bound() {
            Bound::Included(ts) => Bound::Included((key.to_vec(), *ts)),
            Bound::Excluded(ts) => Bound::Excluded((key.to_vec(), *ts)),
            Bound::Unbounded => Bound::Included((key.to_vec(), 0)),
        };
        let key_end = match ts_range.end_bound() {
            Bound::Included(ts) => Bound::Included((key.to_vec(), *ts)),
            Bound::Excluded(ts) => Bound::Excluded((key.to_vec(), *ts)),
            Bound::Unbounded => Bound::Included((key.to_vec(), u64::MAX)),
        };

        column.range((key_start, key_end)).last()
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        let column = self.column_mut(column);
        column.insert((key, ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: &[u8], column: Column, commit_ts: u64) -> Option<Value> {
        // Your code here.
        let column = self.column_mut(column);
        column.remove(&(key.to_vec(), commit_ts))
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        // Your code here.
        let GetRequest { key, start_ts } = req;

        let data = loop {
            let data = self.data.lock().unwrap();
            let lock = data.read(&key, Column::Lock, 0..=start_ts);
            if lock.is_some() {
                self.back_off_maybe_clean_up_lock(start_ts, &key, data);
            } else {
                break data;
            }
        };

        let value = if let Some((_, write)) = data.read(&key, Column::Write, 0..=start_ts) {
            let data_ts = write.as_ts();
            let value = data.read(&key, Column::Data, data_ts..=data_ts).unwrap().1;
            value.as_bytes().to_vec()
        } else {
            vec![]
        };

        Ok(GetResponse { value })
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        // Your code here.

        let PrewriteRequest {
            write,
            primary_key,
            start_ts,
        } = req;
        let Write { key, value } = write.unwrap();

        let success = {
            let mut data = self.data.lock().unwrap();
            let lock = data.read(&key, Column::Lock, 0..);
            let write = data.read(&key, Column::Write, start_ts..);

            if lock.is_some() || write.is_some() {
                false
            } else {
                let lock = if key == primary_key {
                    Value::Timestamp(0)
                } else {
                    Value::Vector(primary_key)
                };
                data.write(key.clone(), Column::Lock, start_ts, lock);
                data.write(key, Column::Data, start_ts, Value::Vector(value));
                true
            }
        };

        Ok(PrewriteResponse { success })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        // Your code here.
        let CommitRequest {
            is_primary,
            key,
            start_ts,
            commit_ts,
        } = req;

        let success = {
            let mut data = self.data.lock().unwrap();
            let lock = data.erase(&key, Column::Lock, start_ts);
            if is_primary {
                if lock.is_some() {
                    data.write(key, Column::Write, commit_ts, Value::Timestamp(start_ts));
                    true
                } else {
                    false
                }
            } else {
                assert!(lock.is_some(), "secondary commit can not fail");
                data.write(key, Column::Write, commit_ts, Value::Timestamp(start_ts));
                true
            }
        };

        Ok(CommitResponse { success })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(
        &self,
        start_ts: u64,
        key: &[u8],
        data: MutexGuard<'_, KvTable>,
    ) {
        // Your code here.
        unimplemented!()
    }
}

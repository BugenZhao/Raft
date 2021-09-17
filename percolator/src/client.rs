#![allow(clippy::needless_collect)]

use std::time::Duration;

use futures::Future;
use labrpc::*;

use crate::msg::*;
use crate::service::{TSOClient, TransactionClient};
use crate::RUNTIME;

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

#[derive(Clone)]
struct Transcation {
    ts: u64,
    writes: Vec<Write>,
}

impl Transcation {
    pub fn new(ts: u64) -> Self {
        Self { ts, writes: vec![] }
    }
}

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,
    current_txn: Option<Transcation>,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {
            tso_client,
            txn_client,
            current_txn: None,
        }
    }

    async fn get_timestamp_async(&self) -> Result<u64> {
        let response = back_off(|| self.tso_client.get_timestamp(&TimestampRequest {})).await;
        response.map(|r| r.ts)
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        RUNTIME.block_on(self.get_timestamp_async())
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        if self.current_txn.is_some() {
            panic!("already started another txn")
        }
        let ts = self.get_timestamp().unwrap();
        self.current_txn.insert(Transcation::new(ts));
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        RUNTIME.block_on(async {
            let start_ts = match &self.current_txn {
                Some(t) => t.ts,
                None => self.get_timestamp_async().await.unwrap(),
            };
            let response = self.txn_client.get(&GetRequest { key, start_ts }).await;
            response.map(|r| r.value)
        })
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        let txn = self.current_txn.as_mut().unwrap();
        txn.writes.push(Write { key, value });
    }

    /// Commits a transaction.
    pub fn commit(&mut self) -> Result<bool> {
        RUNTIME.block_on(self.commit_async())
    }

    async fn commit_async(&mut self) -> Result<bool> {
        // Your code here.
        let Transcation {
            ts: start_ts,
            writes,
        } = self
            .current_txn
            .to_owned()
            .expect("no transaction to commit");
        if writes.is_empty() {
            return Ok(true);
        }
        let primary_key = writes.get(0).unwrap().to_owned().key;

        let keys = writes.iter().map(|w| w.key.to_owned()).collect::<Vec<_>>();

        for write in writes.into_iter() {
            let args = &PrewriteRequest {
                write: Some(write),
                primary_key: primary_key.clone(),
                start_ts,
            };
            let success = back_off(|| self.txn_client.prewrite(args)).await?.success;
            if !success {
                return Ok(false);
            }
        }

        let commit_ts = self.get_timestamp_async().await?;

        for (i, key) in keys.into_iter().enumerate() {
            let is_primary = i == 0;
            let args = &CommitRequest {
                is_primary,
                key,
                start_ts,
                commit_ts,
            };
            let result = back_off(|| self.txn_client.commit(args)).await;
            if is_primary {
                match result {
                    Ok(CommitResponse { success: false }) => return Ok(false),
                    Ok(_) => {}
                    Err(Error::Other(reason)) if reason == "reqhook" => return Ok(false),
                    Err(e) => return Err(e),
                }
            } else {
                // must success, ignore any error
            }
        }

        self.current_txn.take();
        Ok(true)
    }
}

async fn back_off<T, F>(action: impl Fn() -> F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let mut result = action().await;

    for i in 1..=RETRY_TIMES {
        if result.is_ok() {
            return result;
        }
        let duration = Duration::from_millis(BACKOFF_TIME_MS << (i - 1));
        tokio::time::sleep(duration).await;
        result = action().await;
    }

    result
}

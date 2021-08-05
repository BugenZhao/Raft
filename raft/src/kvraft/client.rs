use std::{
    fmt,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Duration,
};

use futures::{
    executor::block_on,
    future::{select, Either},
};
use uuid::Uuid;

use crate::proto::kvraftpb::{self, *};

/// Macro for logging message combined with state of the Raft peer.
macro_rules! clog {
    (level: $level:ident, $cl:expr, $args:expr, $($arg:tt)+) => {
        ::log::$level!("CL [{}] {} [while {:?}]", $cl.name, format_args!($($arg)+), $args)
    };
    ($cl:expr, $args:expr, $($arg:tt)+) => {
        clog!(level: info, $cl, $args, $($arg)+)
    };
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    // You will have to modify this struct.
    id: String,
    next_seq: AtomicU64,
    last_leader: AtomicUsize,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        // You'll have to add code here.
        let id = format!("{}-{:x}", name, Uuid::new_v4().as_u128());

        Clerk {
            name,
            servers,
            id,
            next_seq: 1.into(),
            last_leader: 0.into(),
        }
    }

    fn next_seq(&self) -> u64 {
        self.next_seq.fetch_add(1, Ordering::SeqCst)
    }

    fn cycle_servers(&self) -> impl Iterator<Item = (usize, &KvClient)> {
        self.servers
            .iter()
            .enumerate()
            .cycle()
            .skip(self.last_leader.load(Ordering::Relaxed))
    }

    async fn call(&self, args: KvRequest) -> String {
        let mut iter = self.cycle_servers();

        'outer: loop {
            let (i, server) = iter.next().unwrap();

            'retry: loop {
                clog!(self, args, "request to #{}", i);
                let request = server.op(&args);
                let timeout = futures_timer::Delay::new(Duration::from_millis(1000));
                match select(request, timeout).await {
                    Either::Left((Ok(reply), _)) => {
                        if reply.wrong_leader {
                            continue 'outer;
                        } else {
                            self.last_leader.store(i, Ordering::Relaxed);
                            if !reply.err.is_empty() {
                                clog!(level: warn, self, args, "retry: {}", reply.err);
                                continue 'retry;
                            } else {
                                break 'outer reply.value;
                            }
                        }
                    }
                    Either::Left((Err(e), _)) => {
                        clog!(level: warn, self, args, "try next server: {}", e);
                        continue 'outer;
                    }
                    Either::Right((_, _)) => {
                        clog!(level: warn, self, args, "timeout");
                        continue 'outer;
                    }
                }
            }
        }
    }

    pub async fn get_async(&self, key: String) -> String {
        let args = KvRequest {
            key,
            op: kvraftpb::Op::Get as i32,
            cid: self.id.clone(),
            seq: self.next_seq(),
            ..Default::default()
        };
        self.call(args).await
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { /* do something */ }
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.
        block_on(self.get_async(key))
    }

    pub async fn put_async(&self, key: String, value: String) {
        let args = KvRequest {
            key,
            value,
            op: kvraftpb::Op::Put as i32,
            cid: self.id.clone(),
            seq: self.next_seq(),
        };
        self.call(args).await;
    }

    pub fn put(&self, key: String, value: String) {
        block_on(self.put_async(key, value))
    }

    pub async fn append_async(&self, key: String, value: String) {
        let args = KvRequest {
            key,
            value,
            op: kvraftpb::Op::Append as i32,
            cid: self.id.clone(),
            seq: self.next_seq(),
        };
        self.call(args).await;
    }

    pub fn append(&self, key: String, value: String) {
        block_on(self.append_async(key, value))
    }
}

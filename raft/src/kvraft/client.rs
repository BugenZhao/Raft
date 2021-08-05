use std::{
    fmt,
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use futures::{
    executor::block_on,
    future::{select, Either},
};
use uuid::Uuid;

use crate::proto::kvraftpb::{self, *};

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    // You will have to modify this struct.
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
        Clerk {
            name,
            servers,
            last_leader: 0.into(),
        }
    }

    fn cycle_servers(&self) -> impl Iterator<Item = (usize, &KvClient)> {
        self.servers
            .iter()
            .enumerate()
            .cycle()
            .skip(self.last_leader.load(Ordering::Relaxed))
    }

    async fn call<Req, F, Rep, V>(&self, build_request: Req) -> V
    where
        Req: Fn(&KvClient) -> F,
        F: Future<Output = labrpc::Result<Rep>> + Unpin,
        Rep: Reply<Value = V>,
    {
        let mut iter = self.cycle_servers();

        'outer: loop {
            let (i, server) = iter.next().unwrap();
            'retry: loop {
                let request = build_request(server);
                let timeout = futures_timer::Delay::new(Duration::from_millis(1000));
                match select(request, timeout).await {
                    Either::Left((Ok(reply), _)) => {
                        if reply.wrong_leader() {
                            continue 'outer;
                        } else {
                            self.last_leader.store(i, Ordering::Relaxed);
                            if !reply.error().is_empty() {
                                error!("{}", reply.error());
                                continue 'retry;
                            } else {
                                break 'outer reply.take_value();
                            }
                        }
                    }
                    Either::Left((Err(e), _)) => {
                        error!("{}", e.to_string());
                        continue 'outer;
                    }
                    Either::Right((_, _)) => {
                        error!("timeout");
                        continue 'outer;
                    }
                }
            }
        }
    }

    pub async fn get_async(&self, key: String) -> String {
        let args = GetRequest {
            id: Uuid::new_v4().to_string(),
            key,
        };
        self.call(|s| s.get(&args)).await
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

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    async fn put_append_async(&self, op: Op) {
        // You will have to modify this function.
        let args = match op {
            Op::Put(key, value) => PutAppendRequest {
                id: Uuid::new_v4().to_string(),
                key,
                value,
                op: kvraftpb::Op::Put as i32,
            },
            Op::Append(key, value) => PutAppendRequest {
                id: Uuid::new_v4().to_string(),
                key,
                value,
                op: kvraftpb::Op::Append as i32,
            },
        };
        self.call(|s| s.put_append(&args)).await
    }

    pub async fn put_async(&self, key: String, value: String) {
        self.put_append_async(Op::Put(key, value)).await
    }

    pub fn put(&self, key: String, value: String) {
        block_on(self.put_async(key, value))
    }

    pub async fn append_async(&self, key: String, value: String) {
        self.put_append_async(Op::Append(key, value)).await
    }

    pub fn append(&self, key: String, value: String) {
        block_on(self.append_async(key, value))
    }
}

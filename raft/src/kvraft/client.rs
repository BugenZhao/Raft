use std::fmt;

use futures::executor::block_on;

use crate::proto::kvraftpb::{self, *};

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    // You will have to modify this struct.
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        // You'll have to add code here.
        Clerk { name, servers }
    }

    pub async fn get_async(&self, key: String) -> String {
        let args = GetRequest { key };
        let mut iter = self.servers.iter().cycle();
        let value = loop {
            let server = iter.next().unwrap();
            match server.get(&args).await {
                Ok(reply) => {
                    if reply.wrong_leader || !reply.err.is_empty() {
                        continue;
                    }
                    break reply.value;
                }
                Err(_) => continue,
            }
        };
        value
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
                key,
                value,
                op: kvraftpb::Op::Put as i32,
            },
            Op::Append(key, value) => PutAppendRequest {
                key,
                value,
                op: kvraftpb::Op::Append as i32,
            },
        };
        let mut iter = self.servers.iter().cycle();
        loop {
            let server = iter.next().unwrap();
            match server.put_append(&args).await {
                Ok(reply) => {
                    if reply.wrong_leader || !reply.err.is_empty() {
                        continue;
                    }
                    break;
                }
                Err(_) => continue,
            }
        }
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

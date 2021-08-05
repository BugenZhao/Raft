use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use futures::channel::mpsc::{unbounded, UnboundedReceiver};
use futures::channel::oneshot;
use futures::task::SpawnExt;
use futures::StreamExt;

use super::errors::{Error, Result};
use crate::proto::kvraftpb::*;
use crate::raft::{self, ApplyMsg};
use crate::Executor;

/// Macro for logging message combined with state of the Raft peer.
macro_rules! kvlog {
    (level: $level:ident, $kv:expr, $($arg:tt)+) => {{
        let leader_desc = $kv.raft.is_leader().then(|| "Leader").unwrap_or("Non-leader");
        ::log::$level!("KV [#{} @{} as {}] {}", $kv.me, $kv.raft.term(), leader_desc, format_args!($($arg)+))
    }};
    ($kv:expr, $($arg:tt)+) => {
        kvlog!(level: info, $kv, $($arg)+)
    };
}

#[derive(Debug, Clone)]
enum NotifyReply {
    Get { value: String },
    PutAppend,
}

type Command = KvRequest;
struct Notifier {
    term: u64,
    sender: oneshot::Sender<NotifyReply>,
}

pub struct KvServer {
    pub raft: raft::Node,
    me: usize,
    // snapshot if log grows this big
    #[allow(dead_code)]
    max_raft_state: Option<usize>,
    // Your definitions here.
    store: HashMap<String, String>,
    notifiers: HashMap<u64, Notifier>,
    max_seqs: HashMap<String, u64>,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        max_raft_state: Option<usize>,
    ) -> (KvServer, UnboundedReceiver<ApplyMsg>) {
        // You may need initialization code here.

        let (apply_tx, apply_rx) = unbounded();
        let raft_inner = raft::Raft::new(servers, me, persister, apply_tx);
        let raft = raft::Node::new(raft_inner);

        let kv = KvServer {
            raft,
            me,
            max_raft_state,
            store: HashMap::new(),
            notifiers: HashMap::new(),
            max_seqs: HashMap::new(),
        };
        (kv, apply_rx)
    }
}

impl KvServer {
    fn start(&mut self, command: Command) -> Result<oneshot::Receiver<NotifyReply>> {
        let (tx, rx) = oneshot::channel();
        let (index, term) = self.raft.start(&command).map_err(Error::Raft)?;

        let new = self
            .notifiers
            .insert(index, Notifier { term, sender: tx })
            .is_none();
        assert!(new);

        Ok(rx)
    }

    fn apply(&mut self, msg: ApplyMsg) {
        match msg {
            ApplyMsg::Command { index, command } => {
                let req: KvRequest = labcodec::decode(&command).unwrap();
                kvlog!(self, "apply command: {:?}", req);

                let to_apply = {
                    let max_seq = self.max_seqs.entry(req.cid.clone()).or_default();
                    if req.seq > *max_seq {
                        // only apply new requests
                        *max_seq = req.seq;
                        true
                    } else {
                        false
                    }
                };

                let reply = match req.op() {
                    Op::Put => {
                        if to_apply {
                            kvlog!(self, "Put: {:?}", req);
                            self.store.insert(req.key, req.value);
                        }
                        NotifyReply::PutAppend
                    }
                    Op::Append => {
                        if to_apply {
                            self.store
                                .entry(req.key.clone())
                                .or_default()
                                .push_str(&req.value);
                            let value = self.store.get(&req.key).cloned().unwrap_or_default();
                            kvlog!(self, "After Append: {:?} => {:?}", req, value);
                        }
                        NotifyReply::PutAppend
                    }
                    Op::Get => {
                        let value = self.store.get(&req.key).cloned().unwrap_or_default();
                        kvlog!(self, "Get: {:?} => {:?}", req, value);
                        NotifyReply::Get { value }
                    }
                    Op::Unknown => unreachable!(),
                };

                if let Some(Notifier { term, sender }) = self.notifiers.remove(&index) {
                    if self.raft.is_leader() && term == self.raft.term() {
                        match sender.send(reply) {
                            Ok(_) => kvlog!(self, "send notification"),
                            Err(_) => {
                                kvlog!(level: error, self, "send notification error")
                            }
                        }
                    } else {
                        kvlog!(level: warn, self, "not THAT leader anymore");
                    }
                }
            }
            ApplyMsg::Snapshot { .. } => todo!("snapshot not implemented"),
        }
    }
}

#[derive(Clone)]
pub struct Node {
    // Your definitions here.
    kv: Arc<RwLock<KvServer>>,
    executor: Executor,
}

impl Node {
    pub fn new(kv: KvServer, apply_rx: UnboundedReceiver<ApplyMsg>) -> Node {
        // Your code here.
        let executor = kv.raft.executor.clone();

        let node = Node {
            kv: Arc::new(RwLock::new(kv)),
            executor,
        };
        node.start_applier(apply_rx);

        node
    }

    pub fn start_applier(&self, mut apply_rx: UnboundedReceiver<ApplyMsg>) {
        let kv = Arc::clone(&self.kv);

        self.executor
            .spawn(async move {
                loop {
                    match apply_rx.next().await {
                        Some(msg) => {
                            kv.write().unwrap().apply(msg);
                        }
                        None => {
                            warn!("applier exited");
                            break;
                        }
                    }
                }
            })
            .expect("failed to spawn applier");
    }

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call kvraft::Node::kill only.
        // self.server.kill();

        // Your code here, if desired.
        self.kv.read().unwrap().raft.kill();
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.kv.read().unwrap().raft.term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.kv.read().unwrap().raft.is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        self.kv.read().unwrap().raft.get_state()
    }
}

#[async_trait::async_trait]
impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn op(&self, req: KvRequest) -> labrpc::Result<KvReply> {
        let kv = Arc::clone(&self.kv);
        self.executor
            .spawn_with_handle(async move {
                let mut reply = KvReply::default();
                let start_result = kv.write().unwrap().start(req);

                match start_result {
                    Ok(notifier) => match notifier.await {
                        Ok(NotifyReply::Get { value }) => reply.value = value,
                        Ok(_r) => {}
                        Err(e) => {
                            reply.wrong_leader = true;
                            reply.err = e.to_string();
                        }
                    },
                    Err(Error::Raft(raft::errors::Error::NotLeader)) => reply.wrong_leader = true,
                    Err(e) => reply.err = e.to_string(),
                }

                Ok(reply)
            })
            .unwrap()
            .await
    }
}

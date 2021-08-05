use std::collections::HashMap;
use std::mem;
use std::sync::{Arc, RwLock};

use futures::channel::mpsc::{unbounded, UnboundedReceiver};
use futures::channel::oneshot;
use futures::task::SpawnExt;
use futures::StreamExt;

use super::errors::{Error, Result};
use crate::proto::kvraftpb::*;
use crate::raft::{self, ApplyMsg};
use crate::Executor;

#[macro_export]
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

#[derive(Debug)]
enum NotifyReply {
    Get { value: String },
    Put,
    Append,
}

impl NotifyReply {
    fn value(self) -> String {
        match self {
            NotifyReply::Get { value } => value,
            _ => panic!("called `NotifyReply::value` on a non-`Get` value"),
        }
    }
}

enum ApplyState {
    Applied,
    ToApply(Option<oneshot::Sender<NotifyReply>>),
}

pub struct KvServer {
    pub raft: raft::Node,
    me: usize,
    // snapshot if log grows this big
    #[allow(dead_code)]
    max_raft_state: Option<usize>,
    // Your definitions here.
    store: HashMap<String, String>,
    notifiers: HashMap<String, ApplyState>,
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
        };
        (kv, apply_rx)
    }
}

impl KvServer {
    fn start(&mut self, command: Command) -> Result<oneshot::Receiver<NotifyReply>> {
        self.raft.start(&command).map_err(Error::Raft)?;

        let id = command.id;
        let (tx, rx) = oneshot::channel();
        self.notifiers.insert(id, ApplyState::ToApply(Some(tx)));

        Ok(rx)
    }

    fn start_get(&mut self, mut req: GetRequest) -> Result<oneshot::Receiver<NotifyReply>> {
        let command = Command {
            id: mem::take(&mut req.id),
            value: Some(command::Value::Get(req)),
        };
        self.start(command)
    }

    fn start_put_append(
        &mut self,
        mut req: PutAppendRequest,
    ) -> Result<oneshot::Receiver<NotifyReply>> {
        let command = Command {
            id: mem::take(&mut req.id),
            value: Some(command::Value::PutAppend(req)),
        };
        self.start(command)
    }

    fn apply(&mut self, msg: ApplyMsg) {
        match msg {
            ApplyMsg::Command { index: _, command } => {
                let command = labcodec::decode(&command).unwrap();
                kvlog!(self, "apply command: {:?}", command);
                let Command { id, value } = command;
                let op_value = value.unwrap();
                let state = self
                    .notifiers
                    .entry(id)
                    .or_insert(ApplyState::ToApply(None));

                match state {
                    ApplyState::Applied => {
                        kvlog!(level: warn, self, "ignore applied command");
                    }
                    ApplyState::ToApply(notifier) => {
                        let reply = match op_value {
                            command::Value::PutAppend(req) => match req.op() {
                                Op::Put => {
                                    kvlog!(self, "Put: {:?}", req);
                                    self.store.insert(req.key, req.value);
                                    NotifyReply::Put
                                }
                                Op::Append => {
                                    kvlog!(self, "Append: {:?}", req);
                                    self.store.entry(req.key).or_default().push_str(&req.value);
                                    NotifyReply::Append
                                }
                                _ => unreachable!(),
                            },
                            command::Value::Get(req) => {
                                let value = self.store.get(&req.key).cloned().unwrap_or_default();
                                kvlog!(self, "Get: {:?} => {:?}", req, value);
                                NotifyReply::Get { value }
                            }
                        };
                        if let Some(notifier) = notifier.take() {
                            match notifier.send(reply) {
                                Ok(_) => kvlog!(self, "send notification"),
                                Err(_) => {
                                    kvlog!(level: error, self, "send notification error")
                                }
                            }
                        }
                        // mark as applied
                        *state = ApplyState::Applied;
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
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetReply> {
        // Your code here.

        let kv = Arc::clone(&self.kv);
        self.executor
            .spawn_with_handle(async move {
                let mut reply = GetReply::default();
                let start_result = kv.write().unwrap().start_get(req);

                match start_result {
                    Ok(notifier) => match notifier.await {
                        Ok(r) => reply.value = r.value(),
                        Err(e) => reply.err = e.to_string(),
                    },
                    Err(Error::Raft(raft::errors::Error::NotLeader)) => reply.wrong_leader = true,
                    Err(e) => reply.err = e.to_string(),
                }

                Ok(reply)
            })
            .unwrap()
            .await
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn put_append(&self, req: PutAppendRequest) -> labrpc::Result<PutAppendReply> {
        // Your code here.

        let kv = Arc::clone(&self.kv);
        self.executor
            .spawn_with_handle(async move {
                let mut reply = PutAppendReply::default();
                let start_result = kv.write().unwrap().start_put_append(req);

                match start_result {
                    Ok(notifier) => match notifier.await {
                        Ok(_r) => {}
                        Err(e) => reply.err = e.to_string(),
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

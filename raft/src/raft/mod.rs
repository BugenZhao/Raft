use std::collections::HashSet;
use std::fmt::Display;
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::channel::oneshot;
use futures::executor::ThreadPool;
use futures::task::SpawnExt;
use futures::{select, FutureExt, StreamExt};
use rand::Rng;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(Debug)]
pub enum RoleState {
    Follower,
    Candidate {
        votes: HashSet<usize>,
    },
    Leader {
        next_index: Vec<usize>,
        match_index: Vec<usize>,
    },
}

impl Display for RoleState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let desc = match self {
            RoleState::Follower => "Follower",
            RoleState::Candidate { .. } => "Candidate",
            RoleState::Leader { .. } => "Leader",
        };
        write!(f, "{}", desc)
    }
}

#[derive(Debug)]
pub struct PersistentState {
    current_term: u64,
    voted_for: Option<usize>,
    log: Vec<(u64, Entry)>,
}

impl PersistentState {
    pub fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: vec![Default::default()], // dummy entry at index 0
        }
    }
}

#[derive(Debug)]
pub struct VolatileState {
    commit_index: usize,
    last_applied: usize,
}

impl VolatileState {
    pub fn new() -> Self {
        Self {
            commit_index: 0,
            last_applied: 0,
        }
    }
}

#[derive(Debug)]
pub enum Event {
    ResetTimeout,
    Timeout,
    HeartBeat,
    RequestVoteReply(usize, RequestVoteReply),
    AppendEntriesReply(usize, AppendEntriesReply),
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    // states
    role: RoleState,
    p: PersistentState,
    v: VolatileState,

    // channels
    event_loop_tx: Option<UnboundedSender<Event>>, // should always be Some
    apply_tx: UnboundedSender<ApplyMsg>,

    // executor
    executor: ThreadPool,
}

macro_rules! rlog {
    ($raft:expr, $($arg:tt)+) => {
        ::log::info!("[#{} @{} as {}] {}", $raft.me, $raft.p.current_term, $raft.role, format!($($arg)+))
    };
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_tx: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            role: RoleState::Follower,
            p: PersistentState::new(),
            v: VolatileState::new(),
            event_loop_tx: None,
            apply_tx,
            executor: ThreadPool::new().unwrap(),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf.turn_follower();

        rf
    }

    fn update_term(&mut self, term: u64) {
        if term > self.p.current_term {
            rlog!(self, "update term to {}", term);
            self.p.current_term = term;
            self.p.voted_for = None;
        } else if term < self.p.current_term {
            panic!("update to a lower term")
        }
    }

    fn event_loop_tx(&self) -> &UnboundedSender<Event> {
        self.event_loop_tx.as_ref().expect("no event loop sender")
    }

    fn turn(&mut self, role: RoleState) {
        rlog!(self, "turn to {}", role);
        self.role = role;
    }

    fn turn_follower(&mut self) {
        self.turn(RoleState::Follower);
    }

    fn turn_candidate(&mut self) {
        let votes = [self.me].iter().cloned().collect();
        self.turn(RoleState::Candidate { votes });
    }

    fn turn_leader(&mut self) {
        let next_index = vec![self.p.log.len(); self.peers.len()];
        let match_index = vec![0; self.peers.len()];
        self.turn(RoleState::Leader {
            next_index,
            match_index,
        });
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let peer_clone = peer.clone();
        // let (tx, rx) = channel();
        // peer.spawn(async move {
        //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
        //     tx.send(res);
        // });
        // rx
        // ```
        let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        crate::your_code_here((server, args, tx, rx))
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

impl Raft {
    fn append_entries_args(&self, entries: Vec<Entry>) -> AppendEntriesArgs {
        AppendEntriesArgs {
            term: self.p.current_term,
            leader_id: self.me as u64,
            prev_log_index: self.p.log.len() as u64,
            prev_log_term: self.p.log.last().unwrap().0,
            entries,
            leader_commit_index: self.v.commit_index as u64,
        }
    }

    fn request_vote_args(&self) -> RequestVoteArgs {
        RequestVoteArgs {
            term: self.p.current_term,
            candidate_id: self.me as u64,
            last_log_index: self.p.log.len() as u64 - 1,
            last_log_term: self.p.log.last().unwrap().0,
        }
    }
}

impl Raft {
    fn schedule_event(&self, event: Event) {
        let result = self.event_loop_tx().unbounded_send(event);

        if let Err(e) = result {
            error!("schedule event: {}", e);
        }
    }

    fn handle_event(&mut self, event: Event) {
        if !matches!(event, Event::HeartBeat) {
            rlog!(self, "handle event: {:?}", event);
        }

        match event {
            Event::ResetTimeout => unreachable!(), // already handled by timer
            Event::Timeout => self.handle_timeout(),
            Event::HeartBeat => self.handle_heart_beat(),
            Event::RequestVoteReply(from, reply) => self.handle_request_vote_reply(from, reply),
            Event::AppendEntriesReply(_from, reply) => self.handle_append_entries_reply(reply),
        }
    }

    fn handle_timeout(&mut self) {
        match &mut self.role {
            RoleState::Follower | RoleState::Candidate { .. } => {
                // start new election
                self.turn_candidate();
                self.update_term(self.p.current_term + 1);
                self.p.voted_for = Some(self.me);
                self.schedule_event(Event::ResetTimeout);

                rlog!(self, "start new election");

                for (i, peer) in self.peers.iter().enumerate() {
                    if i == self.me {
                        continue;
                    }
                    let tx = self.event_loop_tx().clone();
                    let fut = peer.request_vote(&self.request_vote_args());
                    self.executor
                        .spawn(async move {
                            if let Ok(reply) = fut.await {
                                tx.unbounded_send(Event::RequestVoteReply(i, reply))
                                    .unwrap();
                                // todo: rx may be closed
                            }
                        })
                        .unwrap();
                }
            }
            _ => {} // no timeout for leader
        }
    }

    fn handle_heart_beat(&mut self) {
        match &mut self.role {
            RoleState::Leader { .. } => {
                // send heart beats
                rlog!(self, "send heart beats");

                for (i, peer) in self.peers.iter().enumerate() {
                    if i == self.me {
                        continue;
                    }
                    let tx = self.event_loop_tx().clone();
                    let fut = peer.append_entries(&self.append_entries_args(vec![]));
                    self.executor
                        .spawn(async move {
                            if let Ok(reply) = fut.await {
                                tx.unbounded_send(Event::AppendEntriesReply(i, reply))
                                    .unwrap();
                            }
                        })
                        .unwrap();
                }
            }
            _ => {}
        }
    }

    fn handle_request_vote_request(
        &mut self,
        args: RequestVoteArgs,
    ) -> labrpc::Result<RequestVoteReply> {
        let vote_granted = {
            if args.term < self.p.current_term {
                false
            } else {
                if args.term > self.p.current_term {
                    self.update_term(args.term);
                    self.turn_follower();
                }

                let id = args.candidate_id as usize;
                if self.p.voted_for.map(|v| v == id) != Some(false) {
                    // if self is candidate, then voted_for is already Some(me)
                    // todo: check log up-to-date
                    self.p.voted_for = Some(id);
                    true
                } else {
                    false
                }
            }
        };

        Ok(RequestVoteReply {
            term: self.p.current_term,
            vote_granted,
        })
    }

    fn handle_request_vote_reply(&mut self, from: usize, reply: RequestVoteReply) {
        if reply.term > self.p.current_term {
            self.update_term(reply.term);
            self.turn_follower();
        }

        match &mut self.role {
            RoleState::Candidate { votes } => {
                if reply.vote_granted && reply.term == self.p.current_term {
                    votes.insert(from);
                    if votes.len() > self.peers.len() / 2 {
                        self.turn_leader();
                        self.handle_heart_beat(); // trigger heart beat immediately
                    }
                }
            }
            _ => {}
        }
    }

    fn handle_append_entries_request(
        &mut self,
        args: AppendEntriesArgs,
    ) -> labrpc::Result<AppendEntriesReply> {
        let success = {
            if args.term < self.p.current_term {
                false
            } else {
                if args.term > self.p.current_term
                    || matches!(self.role, RoleState::Candidate { .. }) // election failed
                        && args.term == self.p.current_term
                {
                    self.update_term(args.term);
                    self.turn_follower();
                }

                match &mut self.role {
                    RoleState::Follower => {
                        self.schedule_event(Event::ResetTimeout);
                        true // todo: log replication
                    }
                    RoleState::Candidate { .. } => {
                        unreachable!("candidate should turn into follower before")
                    }
                    RoleState::Leader { .. } => unreachable!("another leader with same term found"),
                }
            }
        };

        Ok(AppendEntriesReply {
            term: self.p.current_term,
            success,
        })
    }

    fn handle_append_entries_reply(&mut self, reply: AppendEntriesReply) {
        if reply.term > self.p.current_term {
            self.update_term(reply.term);
            self.turn_follower();
        }

        match &mut self.role {
            RoleState::Leader { .. } => {
                // todo: lab 2B +
            }
            _ => {}
        }
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        // let _ = &self.state;
        let _ = &self.apply_tx;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
    event_loop_tx: UnboundedSender<Event>,
    shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    executor: ThreadPool,
}

impl Node {
    /// Create a new raft service.
    pub fn new(mut raft: Raft) -> Node {
        // Your code here.
        let (event_loop_tx, event_loop_rx) = mpsc::unbounded();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        raft.event_loop_tx = Some(event_loop_tx.clone());

        let node = Self {
            raft: Arc::new(Mutex::new(raft)),
            event_loop_tx,
            shutdown_tx: Arc::new(Mutex::new(Some(shutdown_tx))),
            executor: ThreadPool::new().unwrap(),
        };
        node.start_event_loop(event_loop_rx, shutdown_rx);

        node
    }

    fn start_event_loop(
        &self,
        mut event_loop_rx: UnboundedReceiver<Event>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) {
        let raft = Arc::clone(&self.raft);
        let event_loop_tx = self.event_loop_tx.clone();

        self.executor
            .spawn(async move {
                let build_rand_timer = || {
                    futures_timer::Delay::new(Duration::from_millis(
                        rand::thread_rng().gen_range(500, 1000),
                    ))
                    .fuse()
                };
                let build_hb_timer =
                    || futures_timer::Delay::new(Duration::from_millis(150)).fuse();

                let mut timeout_timer = build_rand_timer();
                let mut hb_timer = build_hb_timer();

                loop {
                    select! {
                        event = event_loop_rx.select_next_some() => {
                            match event {
                                Event::ResetTimeout => timeout_timer = build_rand_timer(),
                                event => raft.lock().unwrap().handle_event(event),
                            }
                        }
                        _ = timeout_timer => {
                            event_loop_tx.unbounded_send(Event::Timeout).unwrap();
                            timeout_timer = build_rand_timer();
                        },
                        _ = hb_timer => {
                            event_loop_tx.unbounded_send(Event::HeartBeat).unwrap();
                            hb_timer = build_hb_timer();
                        }
                        _ = shutdown_rx => break, // will cause event_loop_rx to be dropped
                    }
                }
            })
            .expect("failed to spawn event loop");
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        self.raft.lock().unwrap().start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.raft.lock().unwrap().p.current_term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        matches!(self.raft.lock().unwrap().role, RoleState::Leader { .. })
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        if let Some(tx) = self.shutdown_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        let mut raft = self.raft.lock().unwrap();
        rlog!(raft, "rpc: {:?}", args);
        let result = raft.handle_request_vote_request(args);
        rlog!(raft, "rpc reply: {:?}", result);
        result
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let mut raft = self.raft.lock().unwrap();
        rlog!(raft, "rpc: {:?}", args);
        let result = raft.handle_append_entries_request(args);
        rlog!(raft, "rpc reply: {:?}", result);
        result
    }
}

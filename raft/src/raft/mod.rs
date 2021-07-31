use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::channel::oneshot;
use futures::task::SpawnExt;
use futures::{select, FutureExt, StreamExt};
use rand::Rng;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
mod states;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use self::states::*;
use crate::proto::raftpb::*;

pub use self::states::State;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

type RpcResult<T> = labrpc::Result<T>;

#[derive(Debug)]
pub enum Event {
    ElectionTimeout,
    HeartBeat,
    RequestVoteReply {
        from: usize,
        reply: RpcResult<RequestVoteReply>,
    },
    AppendEntriesReply {
        from: usize,
        reply: RpcResult<AppendEntriesReply>,
        new_next_index: usize,
        is_heart_beat: bool, // logging purpose only
    },
}

#[derive(Debug)]
pub enum TimerAction {
    ResetTimeout,
}

type Executor = futures::executor::ThreadPool;
lazy_static::lazy_static! {
    static ref SHARED_EXECUTOR: Executor = Executor::new().unwrap();
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
    timer_action_tx: Option<UnboundedSender<TimerAction>>, // should always be Some
    apply_tx: UnboundedSender<ApplyMsg>,

    // executor
    executor: Executor,
}

macro_rules! rlog {
    (level: $level:ident, $raft:expr, $($arg:tt)+) => {
        ::log::$level!("[#{} @{} as {}] {}", $raft.me, $raft.p.current_term, $raft.role, format!($($arg)+))
    };
    ($raft:expr, $($arg:tt)+) => {
        rlog!(level: info, $raft, $($arg)+)
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
            timer_action_tx: None,
            apply_tx,
            executor: SHARED_EXECUTOR.clone(),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf.turn_follower();

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        let mut data = Vec::new();
        labcodec::encode(&self.p, &mut data).unwrap(); // todo: merge
        self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            rlog!(self, "start without any state");
            return;
        }
        // Your code here (2C).
        self.p = labcodec::decode(data).unwrap();
        rlog!(self, "start with restored state");
    }

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    // (index, term)
    where
        M: labcodec::Message,
    {
        match &self.role {
            RoleState::Leader { .. } => {
                let mut data = vec![];
                labcodec::encode(command, &mut data).map_err(Error::Encode)?;
                let entry = Entry {
                    term: self.p.current_term,
                    data,
                };

                self.p.log.push(entry);

                Ok((self.last_log_index(), self.last_log_term()))
            }
            _ => Err(Error::NotLeader),
        }
    }
}

// utilities
impl Raft {
    fn event_loop_tx(&self) -> &UnboundedSender<Event> {
        self.event_loop_tx.as_ref().expect("no event loop sender")
    }

    fn timer_action_tx(&self) -> &UnboundedSender<TimerAction> {
        self.timer_action_tx
            .as_ref()
            .expect("no timer action sender")
    }

    fn other_peers(&self) -> impl Iterator<Item = (usize, &RaftClient)> {
        self.peers
            .iter()
            .enumerate()
            .filter(move |(i, _)| i != &self.me)
    }

    fn last_log_term(&self) -> u64 {
        self.p.log.last().unwrap().term
    }

    fn last_log_index(&self) -> u64 {
        (self.p.log.len() - 1) as u64
    }

    fn append_entries_args(&self, start_at: usize) -> AppendEntriesArgs {
        let entries = self.p.log[start_at..].iter().cloned().collect();
        let prev_log_index = start_at - 1;

        AppendEntriesArgs {
            term: self.p.current_term,
            leader_id: self.me as u64,
            prev_log_index: prev_log_index as u64,
            prev_log_term: self.p.log[prev_log_index].term,
            entries,
            leader_commit_index: self.v.commit_index as u64,
        }
    }

    fn request_vote_args(&self) -> RequestVoteArgs {
        RequestVoteArgs {
            term: self.p.current_term,
            candidate_id: self.me as u64,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        }
    }
}

// state actions
impl Raft {
    fn update_term(&mut self, term: u64) {
        if term > self.p.current_term {
            rlog!(self, "update term to {}", term);
            self.p.current_term = term;
            self.p.voted_for = None;
        } else if term < self.p.current_term {
            panic!("update to a lower term")
        }
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
}

// actions
impl Raft {
    fn start_new_election(&self) {
        rlog!(self, "start new election");

        for (i, peer) in self.other_peers() {
            let tx = self.event_loop_tx().clone();
            let fut = peer.request_vote(&self.request_vote_args());
            self.executor
                .spawn(async move {
                    let reply = fut.await;
                    let _ = tx.unbounded_send(Event::RequestVoteReply { from: i, reply });
                })
                .unwrap();
        }
    }

    fn heart_beat_sync_log(&self) {
        rlog!(self, "hb, sync log for all");

        if let RoleState::Leader { next_index, .. } = &self.role {
            for (i, peer) in self.other_peers() {
                let tx = self.event_loop_tx().clone();
                let args = self.append_entries_args(next_index[i]);
                let fut = peer.append_entries(&args);
                let new_next_index = self.p.log.len();
                let is_heart_beat = args.entries.is_empty();

                self.executor
                    .spawn(async move {
                        let reply = fut.await;
                        let _ = tx.unbounded_send(Event::AppendEntriesReply {
                            from: i,
                            reply,
                            new_next_index,
                            is_heart_beat,
                        });
                    })
                    .unwrap();
            }
        }
    }

    fn commit_and_apply_up_to(&mut self, new_commit_index: usize) {
        if new_commit_index <= self.v.commit_index {
            return;
        }

        for i in (self.v.commit_index + 1)..=new_commit_index {
            let msg = ApplyMsg {
                command_valid: true,
                command: self.p.log[i].data.clone(),
                command_index: i as u64,
            };
            rlog!(self, "commit and apply msg at {}: {:?}", i, msg.command);
            self.apply_tx.unbounded_send(msg).unwrap();
        }

        self.v.commit_index = new_commit_index;
        self.v.last_applied = new_commit_index; // todo: background real apply?
    }

    fn leader_try_commit(&mut self) {
        if let RoleState::Leader { match_index, .. } = &self.role {
            let mut new_commit_index = self.v.commit_index;

            for n in self.v.commit_index..self.p.log.len() {
                let match_count = self
                    .other_peers()
                    .filter(|(i, _)| match_index[*i] >= n)
                    .count()
                    + 1;
                let majority_match = match_count > self.peers.len() / 2;
                let is_current_term = self.p.log[n].term == self.p.current_term;

                if is_current_term {
                    if majority_match {
                        new_commit_index = n;
                    } else {
                        break;
                    }
                }
            }

            self.commit_and_apply_up_to(new_commit_index);
        }
    }

    fn reset_timeout(&self) {
        let _ = self
            .timer_action_tx()
            .unbounded_send(TimerAction::ResetTimeout);
    }
}

// event handlers
impl Raft {
    fn handle_event(&mut self, event: Event) {
        match (&event, &self.role) {
            (Event::HeartBeat, _) => {}
            (
                Event::AppendEntriesReply {
                    is_heart_beat: true,
                    ..
                },
                _,
            ) => {}
            (Event::ElectionTimeout, RoleState::Leader { .. }) => {}
            _ => rlog!(self, "handle event: {:?}", event),
        }

        match event {
            Event::ElectionTimeout => self.handle_election_timeout(),
            Event::HeartBeat => self.handle_heart_beat(),
            Event::RequestVoteReply { from, reply } => self.handle_request_vote_reply(from, reply),
            Event::AppendEntriesReply {
                from,
                reply,
                new_next_index,
                ..
            } => self.handle_append_entries_reply(from, reply, new_next_index),
        }
    }

    fn handle_election_timeout(&mut self) {
        match &mut self.role {
            RoleState::Follower | RoleState::Candidate { .. } => {
                // start new election
                self.turn_candidate();
                self.update_term(self.p.current_term + 1);
                self.p.voted_for = Some(self.me as u64);
                self.start_new_election();
            }
            _ => {} // no timeout for leader
        }
    }

    fn handle_heart_beat(&mut self) {
        match &mut self.role {
            RoleState::Leader { .. } => {
                // send heart beats
                self.heart_beat_sync_log();
            }
            _ => {} // no heart beat for non-leader
        }
        self.persist(); // persist data
    }

    fn handle_request_vote_request(
        &mut self,
        args: RequestVoteArgs,
    ) -> labrpc::Result<RequestVoteReply> {
        let vote_for = {
            if args.term < self.p.current_term {
                None
            } else {
                if args.term > self.p.current_term {
                    self.update_term(args.term);
                    self.turn_follower();
                }

                let id = args.candidate_id;

                // if self is candidate, then voted_for is already Some(me)
                let not_voted_other = self.p.voted_for.map(|v| v == id) != Some(false);
                // cand's log must be more up-to-date
                let cand_up_to_date = (args.last_log_term, args.last_log_index)
                    >= (self.last_log_term(), self.last_log_index());

                if not_voted_other && cand_up_to_date {
                    Some(id)
                } else {
                    None
                }
            }
        };

        if let Some(v) = vote_for {
            self.p.voted_for = Some(v);
            self.reset_timeout();
        }

        Ok(RequestVoteReply {
            term: self.p.current_term,
            vote_granted: vote_for.is_some(),
        })
    }

    fn handle_request_vote_reply(&mut self, from: usize, reply: RpcResult<RequestVoteReply>) {
        match reply {
            Ok(reply) => {
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
            Err(e) => {
                rlog!(level: warn, self, "request vote -> {} `{}`", from, e);
            }
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
                        self.reset_timeout();

                        // log replication
                        let contains_prev =
                            self.p.log.get(args.prev_log_index as usize).map(|e| e.term)
                                == Some(args.prev_log_term);
                        if !contains_prev {
                            false
                        } else {
                            while self.p.log.len() > args.prev_log_index as usize + 1 {
                                self.p.log.pop();
                            }

                            let mut entries = args.entries;
                            if !entries.is_empty() {
                                rlog!(self, "overwrite {} entries", entries.len());
                                self.p.log.append(&mut entries);
                            }

                            if args.leader_commit_index as usize > self.v.commit_index {
                                let new_commit_index =
                                    args.leader_commit_index.min(self.last_log_index()) as usize;
                                self.commit_and_apply_up_to(new_commit_index);
                            }
                            true
                        }
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

    fn handle_append_entries_reply(
        &mut self,
        from: usize,
        reply: RpcResult<AppendEntriesReply>,
        new_next_index: usize,
    ) {
        match reply {
            Ok(reply) => {
                if reply.term > self.p.current_term {
                    self.update_term(reply.term);
                    self.turn_follower();
                }

                match &mut self.role {
                    RoleState::Leader {
                        next_index,
                        match_index,
                    } => {
                        if reply.success {
                            next_index[from] = new_next_index;
                            match_index[from] = new_next_index - 1;
                            self.leader_try_commit();
                        } else {
                            next_index[from] = next_index[from].saturating_sub(1);
                            // will sync again on next heartbeat
                        }
                    }
                    _ => {}
                }
            }
            Err(e) => {
                rlog!(
                    level: warn,
                    self,
                    "append entries / heart beat -> {} `{}`",
                    from,
                    e
                );
            }
        }
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
    executor: Executor,
}

impl Node {
    /// Create a new raft service.
    pub fn new(mut raft: Raft) -> Node {
        // Your code here.
        let (event_loop_tx, event_loop_rx) = mpsc::unbounded();
        let (timer_action_tx, timer_action_rx) = mpsc::unbounded();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        raft.event_loop_tx = Some(event_loop_tx.clone());
        raft.timer_action_tx = Some(timer_action_tx);

        let executor = raft.executor.clone();

        let node = Self {
            raft: Arc::new(Mutex::new(raft)),
            event_loop_tx,
            shutdown_tx: Arc::new(Mutex::new(Some(shutdown_tx))),
            executor,
        };
        node.start_event_loop(event_loop_rx, shutdown_rx);
        node.start_timer(timer_action_rx);

        node
    }

    fn start_event_loop(
        &self,
        mut event_loop_rx: UnboundedReceiver<Event>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) {
        let raft = Arc::clone(&self.raft);

        self.executor
            .spawn(async move {
                loop {
                    select! {
                        event = event_loop_rx.select_next_some() => {
                            raft.lock().unwrap().handle_event(event);
                        }
                        _ = shutdown_rx => {
                            let raft = raft.lock().unwrap();
                            rlog!(level: warn, raft, "being killed");
                            break; // will cause event_loop_rx to be dropped
                        }
                    }
                }
            })
            .expect("failed to spawn event loop");
    }

    fn start_timer(&self, mut timer_action_rx: UnboundedReceiver<TimerAction>) {
        let event_loop_tx = self.event_loop_tx.clone();

        self.executor
            .spawn(async move {
                let build_timeout_timer = || {
                    futures_timer::Delay::new(Duration::from_millis(
                        rand::thread_rng().gen_range(150, 250),
                    ))
                    .fuse()
                };
                let build_hb_timer = || futures_timer::Delay::new(Duration::from_millis(50)).fuse();

                let mut timeout_timer = build_timeout_timer();
                let mut hb_timer = build_hb_timer();

                loop {
                    select! {
                        action = timer_action_rx.select_next_some() => {
                            match action {
                                TimerAction::ResetTimeout => timeout_timer = build_timeout_timer(),
                            }
                        }
                        _ = timeout_timer => {
                            match event_loop_tx.unbounded_send(Event::ElectionTimeout) {
                                Ok(_) => timeout_timer = build_timeout_timer(),
                                Err(_) => break, // event loop exited
                            }
                        },
                        _ = hb_timer => {
                            match event_loop_tx.unbounded_send(Event::HeartBeat) {
                                Ok(_) => hb_timer = build_hb_timer(),
                                Err(_) => break, // event loop exited
                            }
                        }
                    }
                }
            })
            .expect("failed to spawn timer");
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
        rlog!(raft, "rpc -> {:?}", args);
        let result = raft.handle_request_vote_request(args);
        rlog!(raft, "rpc <- {:?}", result);
        result
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let mut raft = self.raft.lock().unwrap();
        let is_hb = args.entries.is_empty();
        if is_hb {
            rlog!(raft, "rpc -> <heart beat>");
        } else {
            rlog!(raft, "rpc -> {:?}", args);
        }
        let result = raft.handle_append_entries_request(args);
        if !is_hb {
            rlog!(raft, "rpc <- {:?}", result);
        }
        result
    }
}

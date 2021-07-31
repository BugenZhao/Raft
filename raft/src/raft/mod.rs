use std::collections::HashSet;
use std::fmt::Display;
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
    log: Vec<Entry>,
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
    Timeout,
    HeartBeat,
    RequestVoteReply {
        from: usize,
        reply: RequestVoteReply,
    },
    AppendEntriesReply {
        from: usize,
        reply: labrpc::Result<AppendEntriesReply>,
        new_next_index: usize,
    },
    HeartBeatReply {
        from: usize,
        reply: AppendEntriesReply,
    },
}

#[derive(Debug)]
pub enum TimerAction {
    ResetTimeout,
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
    executor: ThreadPool,
}

macro_rules! rlog {
    (level: $level:ident, $raft:expr, $($arg:tt)+) => {
        ::log::$level!("[#{} @{} as {}] {}", $raft.me, $raft.p.current_term, $raft.role, format!($($arg)+))
    };
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
            timer_action_tx: None,
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

    fn timer_action_tx(&self) -> &UnboundedSender<TimerAction> {
        self.timer_action_tx
            .as_ref()
            .expect("no timer action sender")
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

    fn other_peers(&self) -> impl Iterator<Item = (usize, &RaftClient)> {
        self.peers
            .iter()
            .enumerate()
            .filter(move |(i, _)| i != &self.me)
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
                self.sync_log();

                Ok((self.last_log_index(), self.last_log_term()))
            }
            _ => Err(Error::NotLeader),
        }
    }
}


impl Raft {
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

    fn start_new_election(&self) {
        rlog!(self, "start new election");

        for (i, peer) in self.other_peers() {
            let tx = self.event_loop_tx().clone();
            let fut = peer.request_vote(&self.request_vote_args());
            self.executor
                .spawn(async move {
                    if let Ok(reply) = fut.await {
                        let _ = tx.unbounded_send(Event::RequestVoteReply { from: i, reply });
                        // todo: rx may be closed
                    }
                })
                .unwrap();
        }
    }

    fn send_heart_beats(&self) {
        rlog!(self, "send heart beats");

        for (i, peer) in self.other_peers() {
            let tx = self.event_loop_tx().clone();
            let fut = peer.append_entries(&self.append_entries_args(self.p.log.len()));
            self.executor
                .spawn(async move {
                    if let Ok(reply) = fut.await {
                        let _ = tx.unbounded_send(Event::HeartBeatReply { from: i, reply });
                    }
                })
                .unwrap();
        }
    }

    fn sync_log_for_peer(&self, i: usize, delayed: bool) {
        rlog!(self, "sync log for peer {}", i);

        if let RoleState::Leader { next_index, .. } = &self.role {
            let peer = &self.peers[i];
            let tx = self.event_loop_tx().clone();
            let fut = peer.append_entries(&self.append_entries_args(next_index[i]));
            let new_next_index = self.p.log.len();

            self.executor
                .spawn(async move {
                    let reply_result = fut.await;
                    if delayed {
                        futures_timer::Delay::new(Duration::from_millis(500)).await;
                    }
                    let _ = tx.unbounded_send(Event::AppendEntriesReply {
                        from: i,
                        reply: reply_result,
                        new_next_index,
                    });
                })
                .unwrap();
        }
    }

    fn sync_log(&self) {
        rlog!(self, "sync log for all");

        self.other_peers()
            .for_each(|(i, _)| self.sync_log_for_peer(i, false));
    }

    fn commit_up_to_new_index(&mut self, new_commit_index: usize) {
        if new_commit_index <= self.v.commit_index {
            return;
        }

        for i in (self.v.commit_index + 1)..=new_commit_index {
            let msg = ApplyMsg {
                command_valid: true,
                command: self.p.log[i].data.clone(),
                command_index: i as u64,
            };
            rlog!(self, "commit msg at {}: {:?}", i, msg.command);
            self.apply_tx.unbounded_send(msg).unwrap();
        }
        self.v.commit_index = new_commit_index;
    }

    fn try_commit(&mut self) {
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

            self.commit_up_to_new_index(new_commit_index);
        }
    }

    fn reset_timeout(&self) {
        let _ = self
            .timer_action_tx()
            .unbounded_send(TimerAction::ResetTimeout);
    }
}

impl Raft {
    fn handle_event(&mut self, event: Event) {
        if !matches!(event, Event::HeartBeat | Event::HeartBeatReply { .. }) {
            rlog!(self, "handle event: {:?}", event);
        }

        match event {
            Event::Timeout => self.handle_timeout(),
            Event::HeartBeat => self.handle_heart_beat(),
            Event::RequestVoteReply { from, reply } => self.handle_request_vote_reply(from, reply),
            Event::AppendEntriesReply {
                from,
                reply,
                new_next_index,
            } => self.handle_append_entries_reply(from, reply, new_next_index),
            Event::HeartBeatReply { reply, .. } => {
                self.handle_append_entries_heart_beat_reply(reply)
            }
        }
    }

    fn handle_timeout(&mut self) {
        match &mut self.role {
            RoleState::Follower | RoleState::Candidate { .. } => {
                // start new election
                self.turn_candidate();
                self.update_term(self.p.current_term + 1);
                self.p.voted_for = Some(self.me);
                self.reset_timeout();
                self.start_new_election();
            }
            _ => {} // no timeout for leader
        }
    }

    fn handle_heart_beat(&mut self) {
        match &mut self.role {
            RoleState::Leader { .. } => {
                // send heart beats
                self.send_heart_beats();
            }
            _ => {} // no heart beat for non-leader
        }
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

                let id = args.candidate_id as usize;

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
        }

        Ok(RequestVoteReply {
            term: self.p.current_term,
            vote_granted: vote_for.is_some(),
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
                            rlog!(self, "overwrite {} entries", entries.len());
                            self.p.log.append(&mut entries);

                            if args.leader_commit_index as usize > self.v.commit_index {
                                let new_commit_index =
                                    args.leader_commit_index.min(self.last_log_index()) as usize;
                                self.commit_up_to_new_index(new_commit_index);
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

    fn handle_append_entries_heart_beat_reply(&mut self, reply: AppendEntriesReply) {
        if reply.term > self.p.current_term {
            self.update_term(reply.term);
            self.turn_follower();
        }
    }

    fn handle_append_entries_reply(
        &mut self,
        from: usize,
        reply: labrpc::Result<AppendEntriesReply>,
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
                            self.try_commit();
                        } else {
                            next_index[from] = next_index[from].saturating_sub(1);
                            self.sync_log_for_peer(from, false);
                        }
                    }
                    _ => {}
                }
            }
            Err(e) => {
                // resend
                rlog!(
                    level: warn,
                    self,
                    "append entries for {} `{}`, resend after delay",
                    from,
                    e
                );
                self.sync_log_for_peer(from, true);
            }
        }
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        self.persist();
        let _ = &self.persister;
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
        let (timer_action_tx, timer_action_rx) = mpsc::unbounded();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        raft.event_loop_tx = Some(event_loop_tx.clone());
        raft.timer_action_tx = Some(timer_action_tx);

        let node = Self {
            raft: Arc::new(Mutex::new(raft)),
            event_loop_tx,
            shutdown_tx: Arc::new(Mutex::new(Some(shutdown_tx))),
            executor: ThreadPool::new().unwrap(),
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
                        _ = shutdown_rx => break, // will cause event_loop_rx to be dropped
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
                        rand::thread_rng().gen_range(500, 1000),
                    ))
                    .fuse()
                };
                let build_hb_timer =
                    || futures_timer::Delay::new(Duration::from_millis(150)).fuse();

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
                            match event_loop_tx.unbounded_send(Event::Timeout) {
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

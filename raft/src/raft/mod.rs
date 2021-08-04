use std::cmp::Ordering;
use std::sync::{Arc, Mutex, RwLock};
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

/// Message sent to the service.
#[derive(Debug)]
pub enum ApplyMsg {
    /// Ask service to apply this command.
    Command { index: u64, command: Vec<u8> },
    /// Ask service to install this snapshot.
    Snapshot {
        index: u64,
        term: u64,
        snapshot: Vec<u8>,
    },
}

type RpcResult<T> = labrpc::Result<T>;

/// Event run by a Raft peer in a event loop.
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
    ForcePersist,
    InstallSnapshotReply {
        from: usize,
        reply: RpcResult<InstallSnapshotReply>,
        new_next_index: usize,
    },
}

/// Action for Raft peer to control the timer.
#[derive(Debug)]
pub enum TimerAction {
    ResetTimeout,
}

type Executor = futures::executor::ThreadPool;
lazy_static::lazy_static! {
    static ref SHARED_EXECUTOR: Executor = Executor::new().unwrap();
}

/// A single Raft peer.
pub struct Raft {
    /// RPC end points of all peers.
    peers: Vec<RaftClient>,
    /// Object to serialize and deserialize this peer's persisted state.
    persister: Box<dyn Persister>,
    /// This peer's index into peers[].
    me: usize,

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    // states
    /// Role of this peer with its specific states.
    role: RoleState,
    /// Persistent state: `current_form`, `voted_for`, `log`.
    p: PersistentState,
    /// Volatile state: `commit_index`, `last_applied`.
    v: VolatileState,

    // channels
    /// Commit event to handle asynchronously, should always be `Some`.
    event_loop_tx: Option<UnboundedSender<Event>>,
    /// Commit timer action to handle asynchronously, like to reset election timeout, should always be `Some`.
    timer_action_tx: Option<UnboundedSender<TimerAction>>,
    /// Apply commands to the application.
    apply_tx: UnboundedSender<ApplyMsg>,

    /// Shared thread pool executor for background RPC requests.
    executor: Executor,
}

/// Macro for logging message combined with state of the Raft peer.
macro_rules! rlog {
    (level: $level:ident, $raft:expr, $($arg:tt)+) => {
        ::log::$level!("[#{} @{} as {}] {}", $raft.me, $raft.p.current_term, $raft.role, format_args!($($arg)+))
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

        // may initialize from state persisted before a crash
        let raft_state = rf.persister.raft_state();
        rf.restore(&raft_state);

        rf.turn_follower();
        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&self, snapshot: Option<Vec<u8>>) {
        // Your code here (2C).
        let p = bincode::serialize(&self.p).unwrap();

        if let Some(ss) = snapshot {
            self.persister.save_state_and_snapshot(p, ss);
        } else {
            self.persister.save_raft_state(p);
        }
    }

    /// restore previously persisted state.
    fn restore(&mut self, p: &[u8]) {
        // Your code here (2C).
        if let Ok(p) = bincode::deserialize(p) {
            rlog!(self, "restored raft state");
            self.p = p;
            self.v.commit_index = self.p.log.snapshot_last_included_index();
            self.v.last_applied = self.v.commit_index;
        }
    }

    /// The service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log.
    /// Returns immediately with `(index, term)` of the command entry.
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

                self.p.log.push(entry); // will sync on next heartbeat

                Ok((self.p.log.last_index() as u64, self.p.log.last_term()))
            }
            _ => Err(Error::NotLeader),
        }
    }
}

// utilities
impl Raft {
    /// Get a ref to `event_loop_tx`.
    fn event_loop_tx(&self) -> &UnboundedSender<Event> {
        self.event_loop_tx.as_ref().expect("no event loop sender")
    }

    /// Get a ref to `timer_action_tx`.
    fn timer_action_tx(&self) -> &UnboundedSender<TimerAction> {
        self.timer_action_tx
            .as_ref()
            .expect("no timer action sender")
    }

    /// Enumerated peers except `me`.
    fn other_peers(&self) -> impl Iterator<Item = (usize, &RaftClient)> {
        self.peers
            .iter()
            .enumerate()
            .filter(move |(i, _)| i != &self.me)
    }

    /// Make arguments for `AppendEntries` RPC by the index where entries `start_at`.
    fn append_entries_args(&self, start_at: usize) -> Option<AppendEntriesArgs> {
        assert!(start_at >= 1, "log index start from 1");

        let prev_log_index = start_at - 1;
        let prev_log_term = self.p.log.term_at(prev_log_index)?;
        let entries = self.p.log.start_at(start_at)?.cloned().collect();

        Some(AppendEntriesArgs {
            term: self.p.current_term,
            leader_id: self.me as u64,
            prev_log_index: prev_log_index as u64,
            prev_log_term,
            entries,
            leader_commit_index: self.v.commit_index as u64,
        })
    }

    /// Make arguments for `RequestVote` RPC.
    fn request_vote_args(&self) -> RequestVoteArgs {
        RequestVoteArgs {
            term: self.p.current_term,
            candidate_id: self.me as u64,
            last_log_index: self.p.log.last_index() as u64,
            last_log_term: self.p.log.last_term(),
        }
    }

    /// Make arguments for `InstallSnapshot` RPC.
    fn install_snapshot_args(&self) -> InstallSnapshotArgs {
        InstallSnapshotArgs {
            term: self.p.current_term,
            leader_id: self.me as u64,
            last_included_index: self.p.log.snapshot_last_included_index() as u64,
            last_included_term: self.p.log.snapshot_last_included_term(),
            data: self.persister.snapshot(),
        }
    }
}

// state actions
impl Raft {
    /// Update current term and clear `voted_for` state.
    /// WILL NOT touch the `role` state.
    fn update_term(&mut self, term: u64) {
        match term.cmp(&self.p.current_term) {
            Ordering::Less => panic!("update to a lower term"),
            Ordering::Equal => {}
            Ordering::Greater => {
                rlog!(self, "update term to {}", term);
                self.p.current_term = term;
                self.p.voted_for = None;
            }
        }
    }

    /// Turn to new `role`.
    fn turn(&mut self, role: RoleState) {
        rlog!(self, "turn to {}", role);
        self.role = role;
    }

    /// Turn to follower.
    fn turn_follower(&mut self) {
        self.turn(RoleState::Follower);
    }

    /// Turn to candidate, with 1 votes from ourself.
    fn turn_candidate(&mut self) {
        let votes = [self.me].iter().cloned().collect();
        self.turn(RoleState::Candidate { votes });
    }

    /// Turn to leader, with reinitialized leader states.
    fn turn_leader(&mut self) {
        let next_index = vec![self.p.log.next_index(); self.peers.len()];
        let match_index = vec![0; self.peers.len()];
        self.turn(RoleState::Leader {
            next_index,
            match_index,
        });
    }
}

// actions
impl Raft {
    /// Start a new election by requesting votes from all other peers.
    /// Will return immediately, while push all replies into event loop asynchronously.
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

    /// Sync log by appending entries OR installing snapshot to all other peers.
    /// Will return immediately, while push all replies into event loop asynchronously.
    ///
    /// Should ONLY be called by leader on heart beat.
    fn heart_beat_sync_log(&self) {
        rlog!(self, "hb, sync log for all");

        if let RoleState::Leader { next_index, .. } = &self.role {
            for (i, peer) in self.other_peers() {
                let tx = self.event_loop_tx().clone();

                if let Some(args) = self.append_entries_args(next_index[i]) {
                    let fut = peer.append_entries(&args);
                    let new_next_index = self.p.log.next_index();
                    let is_heart_beat = args.entries.is_empty(); // for logging only

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
                } else {
                    // not enough entries in our log, try to install snapshot
                    let args = self.install_snapshot_args();
                    let fut = peer.install_snapshot(&args);

                    // optimistically assume the snapshot to be successfully installed
                    let new_next_index = self.p.log.snapshot_last_included_index() + 1;

                    self.executor
                        .spawn(async move {
                            let reply = fut.await;
                            let _ = tx.unbounded_send(Event::InstallSnapshotReply {
                                from: i,
                                reply,
                                new_next_index,
                            });
                        })
                        .unwrap();
                }
            }
        }
    }

    /// Update `commit_index` and commit (and apply) all log entries up to it.
    fn commit_and_apply_up_to(&mut self, new_commit_index: usize) {
        if new_commit_index <= self.v.commit_index {
            return;
        }

        let range = (self.v.commit_index + 1)..=new_commit_index;
        rlog!(self, "commit and apply msg in {:?}", range);
        for i in range {
            let msg = ApplyMsg::Command {
                index: i as u64,
                command: self.p.log.data_at(i).unwrap().clone(),
            };
            self.apply_tx.unbounded_send(msg).unwrap();
        }

        self.v.commit_index = new_commit_index;
        self.v.last_applied = new_commit_index; // todo: background real apply?
    }

    /// Try to traverse leader's `start_index`s to find new entries that are replicated and commit them.
    ///
    /// Should ONLY be called by leader on receiving `AppendEntries` replies.
    fn leader_try_commit(&mut self) {
        if let RoleState::Leader { match_index, .. } = &self.role {
            let mut new_commit_index = self.v.commit_index;

            // find N as new commit index
            for n in self.v.commit_index..self.p.log.next_index() {
                let match_count = self
                    .other_peers()
                    .filter(|(i, _)| match_index[*i] >= n)
                    .count()
                    + 1;
                let majority_match = match_count > self.peers.len() / 2;
                let is_current_term = self.p.log.term_at(n).unwrap() == self.p.current_term;

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

    /// Tell timer to reset the election timeout.
    fn reset_timeout(&self) {
        let _ = self
            .timer_action_tx()
            .unbounded_send(TimerAction::ResetTimeout);
    }

    /// Service want Raft peer to conditionally install snapshot given from peer
    /// and applied by us previously.
    ///
    /// Returns whether we grant to install this snapshot.
    fn cond_install_peer_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: usize,
        snapshot: Vec<u8>,
    ) -> bool {
        if (last_included_term, last_included_index)
            >= (
                self.p.log.snapshot_last_included_term(),
                self.p.log.snapshot_last_included_index(),
            )
        {
            // up to date, install now
            self.install_snapshot(true, last_included_index, last_included_term, snapshot)
        } else {
            false
        }
    }

    /// Compact the log and set `commit_index` up to the last index of the snapshot.
    ///
    /// Will do nothing if the compaction is failed.
    /// Returns whether the compaction is successful.
    fn install_snapshot(
        &mut self,
        from_peer: bool,
        included_index: usize,
        included_term: u64,
        snapshot: Vec<u8>,
    ) -> bool {
        rlog!(
            level: info,
            self,
            "install snapshot: from peer({}), included_index({})",
            from_peer,
            included_index,
        );
        if self.p.log.compact_to(included_index, included_term) {
            if included_index >= self.v.commit_index {
                self.v.commit_index = included_index;
                self.v.last_applied = included_index;
            }
            self.persist(Some(snapshot)); // persist immediately with snapshot
            true
        } else {
            rlog!(level: warn, self, "failed to install snapshot");
            false
        }
    }
}

// event handlers
impl Raft {
    /// Entrypoint for handling event received from the event loop.
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
            _ => rlog!(self, "handle event: {:?}", event), // log if needed
        }

        match event {
            Event::ElectionTimeout => self.handle_election_timeout(),
            Event::HeartBeat => self.handle_heart_beat(),
            Event::RequestVoteReply { from, reply } => self.handle_request_vote_reply(from, reply),
            Event::AppendEntriesReply {
                from,
                reply,
                new_next_index,
                is_heart_beat: _,
            } => self.handle_append_entries_reply(from, reply, new_next_index),
            Event::ForcePersist => self.persist(None),
            Event::InstallSnapshotReply {
                from,
                reply,
                new_next_index,
            } => self.handle_install_snapshot_reply(from, reply, new_next_index),
        }
    }

    /// Election timeout. Will turn to candidate and start new election.
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

    /// Heart beat. Sync log if we are leader.
    fn handle_heart_beat(&self) {
        match &self.role {
            RoleState::Leader { .. } => {
                // send heart beats
                self.heart_beat_sync_log();
            }
            _ => {} // no heart beat for non-leader
        }
        self.persist(None); // persist data on heart beat
    }

    /// RPC request for `RequestVote`.
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
                // cand's log must be more up-to-date, or deny it
                let cand_up_to_date = (args.last_log_term, args.last_log_index)
                    >= (self.p.log.last_term(), self.p.log.last_index() as u64);

                if not_voted_other && cand_up_to_date {
                    Some(id)
                } else {
                    None
                }
            }
        };

        if vote_for.is_some() {
            self.p.voted_for = vote_for;
            self.reset_timeout(); // reset election timeout on voting
        }

        Ok(RequestVoteReply {
            term: self.p.current_term,
            vote_granted: vote_for.is_some(),
        })
    }

    /// Reply from our prior `RequestVote` RPC request.
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
                                // majority granted
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

    /// RPC request for `AppendEntries`.
    fn handle_append_entries_request(
        &mut self,
        args: AppendEntriesArgs,
    ) -> labrpc::Result<AppendEntriesReply> {
        let (success, conflict_index) = {
            if args.term < self.p.current_term {
                (false, None)
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
                        let our_term = self.p.log.term_at(args.prev_log_index as usize);
                        let contains_prev = our_term == Some(args.prev_log_term);
                        if !contains_prev {
                            let conflict_index = {
                                // for faster back up
                                if args.prev_log_index >= self.p.log.next_index() as u64 {
                                    // our log is shorter than leader's
                                    // simply request from the very first missing one
                                    self.p.log.next_index()
                                } else {
                                    // ignore ALL entries at `our_term`
                                    let our_term = our_term.unwrap();
                                    (0..args.prev_log_index as usize)
                                        .rev()
                                        .find(|i| self.p.log.term_at(*i) != Some(our_term))
                                        .unwrap() // must exists thanks to dummy entry
                                        + 1
                                }
                            };
                            (false, Some(conflict_index))
                        } else {
                            // valid request, pop stale entries and append new entries to be consistent with leader
                            let contains_leader_all =
                                args.entries.iter().enumerate().all(|(i, entry)| {
                                    let index = args.prev_log_index as usize + 1 + i;
                                    self.p.log.term_at(index) == Some(entry.term)
                                });

                            if contains_leader_all {
                                if !args.entries.is_empty() {
                                    rlog!(level: warn, self, "outdated append entries request");
                                }
                            } else {
                                while self.p.log.next_index() > args.prev_log_index as usize + 1 {
                                    let _ = self.p.log.pop_back().expect("entry must exist");
                                }
                                if !args.entries.is_empty() {
                                    rlog!(self, "overwrite {} entries", args.entries.len());
                                }
                                args.entries.into_iter().for_each(|e| self.p.log.push(e));
                            }

                            if args.leader_commit_index as usize > self.v.commit_index {
                                let new_commit_index = (args.leader_commit_index as usize)
                                    .min(self.p.log.last_index());
                                self.commit_and_apply_up_to(new_commit_index);
                            }
                            (true, None)
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
            conflict_index: conflict_index.unwrap_or(0) as u64,
        })
    }

    /// Reply from our prior `AppendEntries` RPC request.
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
                            let preceding = next_index[from].saturating_sub(1).max(1); // log index start from 1
                            if reply.conflict_index == 0 {
                                // invalid hint
                                next_index[from] = preceding;
                            } else {
                                next_index[from] = (reply.conflict_index as usize).min(preceding);
                            }
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

    /// RPC request for `InstallSnapshot`.
    fn handle_install_snapshot_request(
        &mut self,
        args: InstallSnapshotArgs,
    ) -> labrpc::Result<InstallSnapshotReply> {
        if args.term < self.p.current_term {
            // do nothing
        } else {
            if args.term > self.p.current_term {
                self.update_term(args.term);
                self.turn_follower();
            }
            match &mut self.role {
                RoleState::Follower => {
                    self.reset_timeout(); // reset election timeout as if we are receiving an `AppendEntries`

                    // construct an `ApplyMsg` to the service
                    let msg = ApplyMsg::Snapshot {
                        index: args.last_included_index,
                        term: args.last_included_term,
                        snapshot: args.data,
                    };
                    // just send it to service, and it will call our `cond_install_snapshot` soon
                    self.apply_tx.unbounded_send(msg).unwrap();
                }
                _ => {}
            }
        }

        Ok(InstallSnapshotReply {
            term: self.p.current_term,
        })
    }

    /// Reply from our prior `InstallSnapshot` RPC request.
    fn handle_install_snapshot_reply(
        &mut self,
        from: usize,
        reply: RpcResult<InstallSnapshotReply>,
        new_next_index: usize,
    ) {
        match reply {
            Ok(reply) => {
                if reply.term > self.p.current_term {
                    self.update_term(reply.term);
                    self.turn_follower();
                }

                match &mut self.role {
                    RoleState::Leader { next_index, .. } => {
                        // assume the snapshot will be installed successfully anyway
                        // however, do not update match index or try to commit,
                        // which can be perfectly handled by log syncing on heart beat
                        next_index[from] = new_next_index;
                    }
                    _ => {}
                }
            }
            Err(e) => {
                rlog!(level: warn, self, "install snapshot -> {} `{}`", from, e);
            }
        }
    }
}

/// Raft service which triggers event loop, timer and RPC handlers for inner `Raft` instance.
#[derive(Clone)]
pub struct Node {
    // Your code here.
    /// Inner Raft instance.
    raft: Arc<RwLock<Raft>>,
    /// Commit event to handle asynchronously, used by timer
    event_loop_tx: UnboundedSender<Event>,
    /// Notify the event loop to shutdown
    shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    /// Thread pool executor shared with inner Raft instance.
    executor: Executor,
}

impl Node {
    /// Create a new raft service.
    pub fn new(mut raft: Raft) -> Node {
        // Your code here.
        let (event_loop_tx, event_loop_rx) = mpsc::unbounded();
        let (timer_action_tx, timer_action_rx) = mpsc::unbounded();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        raft.event_loop_tx = Some(event_loop_tx.clone()); // for `raft` to commit events itself
        raft.timer_action_tx = Some(timer_action_tx); // for `raft` to control out timer

        let executor = raft.executor.clone();

        let node = Self {
            raft: Arc::new(RwLock::new(raft)),
            event_loop_tx,
            shutdown_tx: Arc::new(Mutex::new(Some(shutdown_tx))),
            executor,
        };
        node.start_event_loop(event_loop_rx, shutdown_rx);
        node.start_timer(timer_action_rx);

        node
    }

    /// Main event loop for the Raft state machine.
    /// Receive events from `event_loop_rx` indefinitely, and call the event handler with MUTABLE reference.
    ///
    /// Receiving message from `shutdown_rx` will break the loop and then cause `event_loop_rx` to be dropped,
    /// which finally make all other tasks holding the `event_loop_tx` to gracefully exit.
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
                            raft.write().unwrap().handle_event(event);
                        }
                        _ = shutdown_rx => {
                            let mut raft = raft.write().unwrap();
                            rlog!(level: warn, raft, "being killed");
                            raft.handle_event(Event::ForcePersist); // trigger persist mannualy
                            break; // will cause event_loop_rx to be dropped
                        }
                    }
                }
            })
            .expect("failed to spawn event loop");
    }

    /// Timer task for the Raft state machine: election timeout and heart beat.
    /// Can be controlled by actions from `timer_action_rx`.
    ///
    /// All timer events will be sent through `event_loop_tx` and then processed by the event loop.
    /// Thus the timer expects the event loop not to be so "crowded" to ensure the real-time.
    fn start_timer(&self, mut timer_action_rx: UnboundedReceiver<TimerAction>) {
        let event_loop_tx = self.event_loop_tx.clone();

        self.executor
            .spawn(async move {
                let build_timeout_timer = || {
                    futures_timer::Delay::new(Duration::from_millis(
                        rand::thread_rng().gen_range(300, 500),
                    ))
                    .fuse()
                };
                let build_hb_timer =
                    || futures_timer::Delay::new(Duration::from_millis(100)).fuse();

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
        self.raft.write().unwrap().start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        self.raft.read().unwrap().p.current_term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        matches!(self.raft.read().unwrap().role, RoleState::Leader { .. })
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the service says it has created a snapshot that has
    /// all info up to and including index. this means the
    /// service no longer needs the log through (and including)
    /// that index. Raft should now trim its log as much as possible.
    pub fn snapshot(&mut self, index: u64, snapshot: Vec<u8>) {
        // Your code here (2D).
        let mut raft = self.raft.write().unwrap();
        let term = raft.p.log.term_at(index as usize).unwrap();
        raft.install_snapshot(false, index as usize, term, snapshot);
    }

    /// A service wants to switch to snapshot.  Only do so if Raft hasn't
    /// have more recent info since it communicate the snapshot on applyCh.
    pub fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: Vec<u8>,
    ) -> bool {
        // Your code here (2D).
        self.raft.write().unwrap().cond_install_peer_snapshot(
            last_included_term,
            last_included_index as usize,
            snapshot,
        )
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
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    // bugen: sorry for my `Mutex::lock`ing :(

    /// RPC service handler for `RequestVote`.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        let mut raft = self.raft.write().unwrap();
        rlog!(raft, "rpc -> {:?}", args);
        let result = raft.handle_request_vote_request(args);
        rlog!(raft, "rpc <- {:?}", result);
        result
    }

    /// RPC service handler for `AppendEntries`.
    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let mut raft = self.raft.write().unwrap();
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

    /// RPC service handler for `InstallSnapshot`.
    async fn install_snapshot(
        &self,
        args: InstallSnapshotArgs,
    ) -> labrpc::Result<InstallSnapshotReply> {
        let mut raft = self.raft.write().unwrap();
        rlog!(raft, "rpc -> {:?}", args);
        let result = raft.handle_install_snapshot_request(args);
        rlog!(raft, "rpc <- {:?}", result);
        result
    }
}

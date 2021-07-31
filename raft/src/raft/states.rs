use crate::proto::raftpb::*;
use std::{collections::HashSet, fmt::Display};

/// State of a raft peer. (for testing)
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

#[derive(Message)]
pub struct PersistentState {
    #[prost(uint64, tag = "1")]
    pub current_term: u64,
    #[prost(uint64, optional, tag = "2")]
    pub voted_for: Option<u64>,
    #[prost(message, repeated, tag = "3")]
    pub log: Vec<Entry>,
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
    pub commit_index: usize,
    pub last_applied: usize,
}

impl VolatileState {
    pub fn new() -> Self {
        Self {
            commit_index: 0,
            last_applied: 0,
        }
    }
}

use crate::proto::raftpb::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fmt::Display};

/// State of a raft peer (for testing).
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

/// Role of a peer with its specific states.
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

/// Persistent state of a raft peer.
#[derive(Serialize, Deserialize)]
pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<u64>,
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

/// Volatile state of a raft peer.
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

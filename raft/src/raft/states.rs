use crate::proto::raftpb::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashSet, VecDeque},
    fmt::Display,
};

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

#[derive(Serialize, Deserialize)]
pub struct Log {
    inner: VecDeque<Entry>,
    offset: usize,
    snapshot_last_included_term: u64,
}

impl Log {
    pub fn new() -> Self {
        let dummy = Entry::default();
        Self {
            inner: vec![dummy].into(),
            offset: 0,
            snapshot_last_included_term: 0,
        }
    }

    pub fn push(&mut self, entry: Entry) {
        self.inner.push_back(entry);
    }

    pub fn get(&self, index: usize) -> Option<&Entry> {
        if self.exists_index(index) {
            self.inner.get(self.offset_index(index))
        } else {
            None
        }
    }

    pub fn next_index(&self) -> usize {
        self.inner.len() + self.offset
    }

    pub fn last_index(&self) -> usize {
        self.next_index() - 1
    }

    pub fn last_term(&self) -> u64 {
        self.inner
            .back()
            .map(|e| e.term)
            .unwrap_or(self.snapshot_last_included_term)
    }

    pub fn pop_back(&mut self) -> Option<Entry> {
        self.inner.pop_back()
    }

    pub fn start_at(&self, index: usize) -> Option<impl Iterator<Item = &Entry>> {
        if self.exists_index(index) {
            Some(self.inner.iter().skip(self.offset_index(index)))
        } else {
            None
        }
    }

    pub fn exists_index(&self, index: usize) -> bool {
        index >= self.offset
    }

    fn offset_index(&self, index: usize) -> usize {
        index - self.offset
    }
}

/// Persistent state of a raft peer.
#[derive(Serialize, Deserialize)]
pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Log,
}

impl PersistentState {
    pub fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: Log::new(),
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

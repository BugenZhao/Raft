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
        /// Received votes from peers.
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

/// Represents the log of a Raft peer, with snapshot support.
#[derive(Serialize, Deserialize, Debug)]
pub struct Log {
    /// In memory log entries.
    inner: VecDeque<Entry>,
    /// Length of entries in snapshot (i.e., not in `inner`).
    in_snapshot_len: usize,
    /// Term of the last entry in snapshot.
    snapshot_last_included_term: u64,
}

impl Log {
    /// Creates an empty log.
    pub fn new() -> Self {
        Self {
            inner: VecDeque::new(),
            in_snapshot_len: 1, // log index starts from 1 by this dummy snapshot
            snapshot_last_included_term: 0,
        }
    }

    /// Term of the last entry in snapshot.
    pub fn snapshot_last_included_term(&self) -> u64 {
        self.snapshot_last_included_term
    }

    /// Index of the last entry in snapshot.
    pub fn snapshot_last_included_index(&self) -> usize {
        self.in_snapshot_len - 1
    }

    /// Index of the next entry slot.
    pub fn next_index(&self) -> usize {
        self.inner.len() + self.in_snapshot_len
    }

    /// Index of the last entry.
    pub fn last_index(&self) -> usize {
        self.next_index() - 1
    }

    /// Term of the last entry.
    pub fn last_term(&self) -> u64 {
        self.inner
            .back()
            .map_or(self.snapshot_last_included_term, |e| e.term)
    }

    /// Get the ref to the entry at given index.
    ///
    /// The index is given as a absolute index of the log.
    /// Thus, the desired entry may be saved into the snapshot and not in the log anymore,
    /// which will be returned as `None`.
    fn get(&self, index: usize) -> Option<&Entry> {
        self.offset_index(index)
            .and_then(|index| self.inner.get(index))
    }

    /// Get the term of the entry at given index.
    ///
    /// Compared to `get`, this method is also able to get the last included term in the snapshot.
    pub fn term_at(&self, index: usize) -> Option<u64> {
        self.get(index)
            .map(|e| e.term)
            .or((index == self.in_snapshot_len - 1).then(|| self.snapshot_last_included_term))
    }

    /// Get the data of the entry at given index.
    pub fn data_at(&self, index: usize) -> Option<&Vec<u8>> {
        self.get(index).map(|e| &e.data)
    }

    /// Get an iterator of log starting at the given index.
    pub fn start_at(&self, index: usize) -> Option<impl Iterator<Item = &Entry>> {
        self.offset_index(index)
            .map(|index| self.inner.iter().skip(index))
    }

    /// Convert an absolute index into `inner`'s index
    fn offset_index(&self, index: usize) -> Option<usize> {
        index.checked_sub(self.in_snapshot_len)
    }

    /// Push an entry to the log.
    pub fn push(&mut self, entry: Entry) {
        self.inner.push_back(entry);
    }

    /// Pop the last entry in log.
    pub fn pop_back(&mut self) -> Option<Entry> {
        self.inner.pop_back()
    }

    /// Compact the log to the given index.
    /// This method is often called after a snapshot is created,
    /// when there'is no need to keep old entries in memory anymore.
    ///
    /// Returns whether the compaction is successful.
    pub fn compact_to(&mut self, included_index: usize, included_term: u64) -> bool {
        if included_index + 1 >= self.in_snapshot_len {
            let n_compact = included_index + 1 - self.in_snapshot_len;
            for _ in 0..n_compact {
                self.inner.pop_front();
            }
            self.in_snapshot_len = included_index + 1;
            self.snapshot_last_included_term = included_term;
            true
        } else {
            false
        }
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

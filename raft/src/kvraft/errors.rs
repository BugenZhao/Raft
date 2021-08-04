use std::{error, fmt, result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Raft(crate::raft::errors::Error),
    NoLeader,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::Raft(e) => e.source(),
            Error::NoLeader => None,
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

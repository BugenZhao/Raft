#![deny(clippy::all)]
#![allow(clippy::single_match)]

#[allow(unused_imports)]
#[macro_use]
extern crate log;
#[allow(unused_imports)]
#[macro_use]
extern crate prost_derive;

pub mod kvraft;
mod proto;
pub mod raft;

pub type Executor = futures::executor::ThreadPool;
lazy_static::lazy_static! {
    pub static ref SHARED_EXECUTOR: Executor = Executor::new().unwrap();
}

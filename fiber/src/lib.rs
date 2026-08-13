#![doc = include_str!("compile_fail.md")]
#![warn(unreachable_pub)]

pub mod abi;
pub mod context;
pub mod extensions;
pub mod file;
pub mod net;
mod raw;
pub mod task;
pub mod wait;

use task::storage;

type TaskKey<'d, Tag> = storage::Id<'d, Tag>;

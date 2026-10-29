use crate::{
    io::{reactor::complete, socket::option::tuning},
    memory,
    table::token,
};

/// Host completion types retained by the ring and option-storage owner.
pub trait Context {
    type Map: complete::Map;
    type Observation: Copy + Eq;
    type Selection: Copy + Eq;
    type Token: token::Codec;
    type Layout: memory::raw::Receive;
    type Tuning: tuning::Create + tuning::Quiesce;
}

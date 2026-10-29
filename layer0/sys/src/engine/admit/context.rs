use crate::io::{file, request};

/// The exact native request families and host flight identity being admitted.
pub trait Context {
    type Native: super::Native + file::File + request::Source<Self::Inputs>;
    type Inputs: request::Context;
    type Key: Copy;
    type Failure;
}

use crate::{
    Errno,
    io::{reactor::complete, socket::accept},
};

/// Typed read completions delivered into an already reserved queue entry.
pub trait Complete: complete::Event + Sized {
    type Accepted;
    type Buffer: crate::Buffer;
    type Flow: Copy + Eq;

    const LAST: Self::Flow;
    const MORE: Self::Flow;

    fn accepted(key: Self::Key, accepted: Self::Accepted, flow: Self::Flow) -> Self;
    fn rejected(key: Self::Key, cause: accept::Error, flow: Self::Flow) -> Self;
    fn received(key: Self::Key, buffer: Self::Buffer, flow: Self::Flow) -> Self;
    fn control(key: Self::Key, result: Result<(), Errno>, flow: Self::Flow) -> Self;
}

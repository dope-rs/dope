use crate::Errno;

/// Exact descriptor-table absence or the result of the one owned close syscall.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum File<M> {
    Missing(M),
    Closed(Result<(), Errno>),
}

/// Synchronous retirement preserves descriptor and reactor failures separately.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Completed<S, M, F> {
    pub slot: S,
    pub file: File<M>,
    pub reactor: Result<(), F>,
}

/// The native retirement command now owns one submitted queue cell.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Submitted;

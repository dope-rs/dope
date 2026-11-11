mod release;
pub use release::Release;

/// Current cleanup evidence retains its native storage or canary failure.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Failure<S, W> {
    Storage(S),
    Wait(W),
}

/// Failed admission retains either probe cleanup or registered-file removal.
#[derive(Debug)]
pub enum Rejection<C, R> {
    Cleanup(C),
    Release(R),
}

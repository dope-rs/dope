use crate::{
    probe::{State, accept, session},
    ring::{self, register},
};

mod release;
pub use release::Release;

type Stage<S> = State<S, <S as session::Session>::Pending>;
type Direct<C> = <<C as Cleanup>::Session as session::Empty>::Direct;
type Occupied<C> = <<C as Cleanup>::Session as session::Empty>::Occupied;

/// Current cleanup evidence retains its native storage or canary failure.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Failure<S, W> {
    Storage(S),
    Wait(W),
}

/// Failed probe cleanup retains every kernel borrow until it can close safely.
pub trait Cleanup: Sized {
    type Primary: Copy;
    type Ring: ring::Owner<File = Self::File>;
    type File: register::Occupied;
    type Accept: accept::Accept<Ring = Self::Ring, File = Self::File>;
    type Session: session::Empty<Ring = Self::Ring>;
    type Error;

    fn raw(ring: Self::Ring, primary: Self::Primary) -> Result<Self::Primary, Self>;
    fn file(
        ring: Self::Ring,
        primary: Self::Primary,
        file: Self::File,
    ) -> Result<Self::Primary, Self>;
    fn accept(
        state: Self::Accept,
        primary: Self::Primary,
        file: Self::File,
    ) -> Result<Self::Primary, Self>;
    fn session(state: Self::Session, primary: Self::Primary) -> Result<Self::Primary, Self>;
    fn receive(state: Stage<Occupied<Self>>, primary: Self::Primary)
    -> Result<Self::Primary, Self>;
    fn socket(state: Direct<Self>, primary: Self::Primary) -> Result<Self::Primary, Self>;
    fn option(state: Stage<Direct<Self>>, primary: Self::Primary) -> Result<Self::Primary, Self>;
    fn primary(&self) -> Self::Primary;
    fn failure(&self) -> Self::Error;
    fn try_close(self, report: impl FnMut(Self::Error)) -> Result<Self::Primary, Self>;
}

/// Failed admission retains either probe cleanup or registered-file removal.
#[derive(Debug)]
pub enum Rejection<C, R> {
    Cleanup(C),
    Release(R),
}

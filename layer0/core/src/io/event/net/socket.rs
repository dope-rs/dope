use std::mem;

use crate::{
    driver::{route, route::kind},
    io::{event::entry::Entry, fd::handles, socket},
};

pub enum Failure {
    Create(socket::Create),
    Retired,
}

pub enum Outcome<'d> {
    Created(handles::CreatedSlot<'d>),
    Failed(Failure),
}

pub enum Finished<'d> {
    Created(handles::Descriptor<'d>),
    Failed(Failure),
}

/// A socket-creation completion and its affine driver target.
///
/// Safe code cannot mint completion authority.
///
///
/// Completion authority is consumed exactly once.
///
#[repr(transparent)]
pub struct Completion<'d>(Entry<'d, kind::Socket, Outcome<'d>>);

/// Exact socket completion and creation authority that could not be joined.
pub struct Mismatch<'d, State> {
    completion: Completion<'d>,
    creating: handles::Creation<'d, State>,
}

impl<'d, State> Mismatch<'d, State> {
    fn new(done: Completion<'d>, creating: handles::Creation<'d, State>) -> Self {
        Self {
            completion: done,
            creating,
        }
    }

    fn from_created(
        target: route::Erased<'d, kind::Socket>,
        cause: (handles::Creation<'d, State>, handles::CreatedSlot<'d>),
    ) -> Self {
        let (creating, made) = cause;
        Self::new(
            Completion(Entry::new(target, Outcome::Created(made))),
            creating,
        )
    }

    /// Reclaims the unmatched completion and returns the still-live
    /// submission half unchanged.
    pub fn reclaim(self) -> handles::Creation<'d, State> {
        let Self {
            completion: done,
            creating,
        } = self;
        done.reclaim();
        creating
    }
}

impl<'d> Completion<'d> {
    pub(in crate::io) const fn new(
        target: route::Erased<'d, kind::Socket>,
        outcome: Outcome<'d>,
    ) -> Self {
        Self(Entry::new(target, outcome))
    }

    pub const fn token(&self) -> route::Erased<'d, kind::Socket> {
        self.0.target()
    }

    pub fn try_finish<State>(
        self,
        creating: handles::Creation<'d, State>,
    ) -> Result<Finished<'d>, Mismatch<'d, State>> {
        let (target, outcome) = self.0.into_parts();
        if !target.matches(creating.target()) {
            let done = Self(Entry::new(target, outcome));
            return Err(Mismatch::new(done, creating));
        }
        match outcome {
            Outcome::Created(made) => {
                let join = match made.try_join(creating) {
                    Ok(join) => join,
                    Err(cause) => return Err(Mismatch::from_created(target, cause)),
                };
                Ok(Finished::Created(join.activate()))
            }
            Outcome::Failed(failure) => {
                creating.detach();
                Ok(Finished::Failed(failure))
            }
        }
    }

    /// Reclaims any descriptor authority carried by an unconsumed completion.
    pub fn reclaim(self) {
        drop(self);
    }

    pub fn into_parts(self) -> (route::Erased<'d, kind::Socket>, Outcome<'d>) {
        self.0.into_parts()
    }
}

const _: () = {
    assert!(mem::size_of::<Failure>() == mem::size_of::<socket::Create>());
    assert!(mem::align_of::<Failure>() == mem::align_of::<socket::Create>());
    assert!(mem::size_of::<Outcome<'_>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Outcome<'_>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Finished<'_>>() == mem::size_of::<Outcome<'_>>());
    assert!(mem::align_of::<Finished<'_>>() == mem::align_of::<Outcome<'_>>());
    assert!(
        mem::size_of::<Mismatch<'_, ()>>()
            == mem::size_of::<(Completion<'_>, handles::Creation<'_, ()>)>()
    );
};

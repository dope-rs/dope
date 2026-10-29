use std::{ops, os::fd, time};

use crate::{
    Budget, Errno,
    io::{
        reactor,
        reactor::ready::{self, delivery::Event},
    },
    table,
};

pub trait Poll<S: table::raw::Slots>: Sized + fd::AsFd {
    type Failure: ready::Failure<Index = S::Index, Missing = S::Missing>;
    type Files: table::Files<S>;
    type Storage;
    type Event<'a>: Event;
    type Events<'a>: Iterator<Item = Self::Event<'a>>;

    fn try_new() -> Result<Self, reactor::Create>;
    fn storage() -> Self::Storage;
    fn next_lane(&mut self) -> usize;
    fn revoke(&mut self, files: &mut Self::Files) -> Result<(), ready::Failures<Self::Failure>>;
    fn is_failed(&self) -> bool;
    fn check(&self) -> Result<(), Self::Failure>;
    /// Runs native work only while healthy, retaining its exact terminal failure.
    fn execute<T, E>(
        &mut self,
        run: impl FnOnce() -> Result<T, E>,
        classify: impl FnOnce(E) -> Self::Failure,
    ) -> ops::ControlFlow<(), T>;
    fn wait<'a>(
        &mut self,
        files: &mut Self::Files,
        events: &'a mut Self::Storage,
        limit: usize,
        timeout: Option<time::Duration>,
        changes: &mut impl Budget,
    ) -> Result<Self::Events<'a>, Self::Failure>;
    fn arm_shutdown(&self, fd: fd::BorrowedFd<'_>, data: S::Data) -> Result<(), Errno>;
}

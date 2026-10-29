use std::marker;

use crate::client::{attempt::mode, side};

pub(in crate::client::conn::engine) struct Phase<M, X> {
    next: u8,
    profile: marker::PhantomData<fn() -> (M, X)>,
}

pub(in crate::client::conn::engine) trait Sequence {
    type Step;

    const COUNT: usize;

    fn advance(&mut self) -> Self::Step;
}

#[derive(Clone, Copy)]
pub(in crate::client::conn::engine) enum Local {
    Dirty,
    Cancellations,
    Submission,
    Liveness,
    Credit,
}

#[derive(Clone, Copy)]
pub(in crate::client::conn::engine) enum Service {
    Dirty,
    Submission,
    Liveness,
    Credit,
}

impl Local {
    pub(super) const COUNT: usize = 5;
}

impl Service {
    pub(super) const COUNT: usize = 4;
}

impl<M, X> Phase<M, X> {
    pub(in crate::client::conn::engine) const fn empty() -> Self {
        Self {
            next: 0,
            profile: marker::PhantomData,
        }
    }

    fn take(&mut self, count: u8) -> u8 {
        let current = self.next;
        self.next = if current + 1 == count { 0 } else { current + 1 };
        current
    }
}

impl<X> Sequence for Phase<mode::Local, X> {
    type Step = Local;

    const COUNT: usize = Local::COUNT;

    fn advance(&mut self) -> Self::Step {
        match self.take(Self::COUNT as u8) {
            0 => Local::Dirty,
            1 => Local::Cancellations,
            2 => Local::Submission,
            3 => Local::Liveness,
            _ => Local::Credit,
        }
    }
}

impl Sequence for Phase<mode::Service, side::Disabled> {
    type Step = Service;

    const COUNT: usize = Service::COUNT;

    fn advance(&mut self) -> Self::Step {
        match self.take(Self::COUNT as u8) {
            0 => Service::Dirty,
            1 => Service::Submission,
            2 => Service::Liveness,
            _ => Service::Credit,
        }
    }
}

impl<R> Sequence for Phase<mode::Service, side::Enabled<R>> {
    type Step = Local;

    const COUNT: usize = Local::COUNT;

    fn advance(&mut self) -> Self::Step {
        match self.take(Self::COUNT as u8) {
            0 => Local::Dirty,
            1 => Local::Cancellations,
            2 => Local::Submission,
            3 => Local::Liveness,
            _ => Local::Credit,
        }
    }
}

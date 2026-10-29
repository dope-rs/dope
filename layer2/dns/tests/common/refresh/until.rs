use std::{cell::Cell, task::Poll, time::Duration};

use dope::{
    core::{
        dope_sys::io::socket::record::Record,
        platform::{channel::Channel, poll},
    },
    runtime::executor::root,
};

use crate::common::refresh::server::{self, Failure};

pub(crate) struct Until<'a, 'fd> {
    changes: &'a Cell<u32>,
    before: u32,
    signal: &'fd Channel,
    events: &'a mut poll::Set<'fd>,
    delay: Option<Duration>,
    lost: u8,
}

impl<'a, 'fd> Until<'a, 'fd> {
    pub(crate) fn new(
        changes: &'a Cell<u32>,
        signal: &'fd Channel,
        events: &'a mut poll::Set<'fd>,
        delay: Option<Duration>,
    ) -> Self {
        Self {
            changes,
            before: changes.get(),
            signal,
            events,
            delay,
            lost: 0,
        }
    }
}

impl<'d> root::Root<'d> for Until<'_, '_> {
    type Output = Result<Option<Duration>, Failure>;

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<Self::Output> {
        let (this, wake, _, _) = context.into_parts();
        let this = this.get_mut();
        loop {
            match this.events.poll(0) {
                Ok(None) => break,
                Ok(Some(_)) => {}
                Err(error) if error.is_intr() => continue,
                Err(error) => return Poll::Ready(Err(Failure::Poll(error))),
            }
            match this.signal.recv() {
                Record::Value(server::FOUR) => this.lost |= 1,
                Record::Value(server::SIX) => this.lost |= 2,
                Record::Value(server::PAUSE) => {
                    return Poll::Ready(this.delay.map(Some).ok_or(Failure::Alias));
                }
                record => return Poll::Ready(Err(Failure::Record(record))),
            }
        }
        if this.changes.get() != this.before || this.lost == 3 {
            Poll::Ready(Ok(None))
        } else {
            wake.wake();
            Poll::Pending
        }
    }
}

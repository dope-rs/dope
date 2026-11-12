use std::task;

pub(in crate::client) use change::Change;
use dope_core::{
    driver::{
        self,
        life::finish::Finish,
        route::{Identity, table},
        schedule::{
            clock::{self, timer},
            ready::{self, wake},
        },
    },
    io::socket,
};
use dope_net::link::oneshot;
pub(in crate::client::conn::engine) use egress::Egress;
pub(in crate::client) use links::Links;
use o3::base::time::Positive;

use crate::{
    app::creation,
    client::{
        app::{self, resume},
        conn,
        deadline::book,
        side,
        side::{mode::Mode, registry},
    },
};

pub(super) mod refresh;
pub(super) mod turn;

pub(in crate::client) struct State<'d, ID: Identity, P, X>
where
    P: socket::Prepared,
    X: Mode<'d, ID>,
{
    book: book::Book<'d, ID>,
    alarm: timer::Alarm<'d>,
    io: X::Transport<P>,
}

pub(in crate::client) enum Observation<'d> {
    Quiescent,
    Awaiting {
        window: Positive,
        seen: clock::Moment<'d>,
    },
}

pub(in crate::client) trait Inspection<'d, ID: Identity, A>
where
    A: app::Application<'d, ID>,
{
    const RESUMABLE: bool = false;

    fn wake(_: &mut A, _: &mut A::Conn) -> bool {
        false
    }

    fn inspect<O>(
        app: &A,
        conn: conn::Ref<'_, 'd, ID, A::Wire, A::Conn, O>,
        base: Positive,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Observation<'d>;
}

impl<'d, ID: Identity, A> Inspection<'d, ID, A> for resume::Complete
where
    A: app::Receive<'d, ID, Continuation = Self> + app::Application<'d, ID>,
{
    fn inspect<O>(
        _: &A,
        _: conn::Ref<'_, 'd, ID, A::Wire, A::Conn, O>,
        _: Positive,
        _: &mut driver::Context<'_, 'd>,
    ) -> Observation<'d> {
        Observation::Quiescent
    }
}

impl<'d, ID: Identity, A> Inspection<'d, ID, A> for resume::Retained
where
    A: app::Resumable<'d, ID, Continuation = Self> + app::Application<'d, ID>,
{
    const RESUMABLE: bool = true;

    fn wake(app: &mut A, conn: &mut A::Conn) -> bool {
        matches!(app::Resumable::wake(app, conn), app::Wake::Ready)
    }

    fn inspect<O>(
        app: &A,
        conn: conn::Ref<'_, 'd, ID, A::Wire, A::Conn, O>,
        base: Positive,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Observation<'d> {
        let seen = conn.last_recv();
        match app::Resumable::inbound(app, conn, base, driver) {
            app::Inbound::Quiescent => Observation::Quiescent,
            app::Inbound::Awaiting(window) => Observation::Awaiting { window, seen },
        }
    }
}

pub(in crate::client) trait Schedule<'d, ID: Identity, P>:
    Mode<'d, ID> + Sized
where
    P: socket::Prepared,
{
    fn earliest(state: &State<'d, ID, P, Self>) -> Option<clock::Deadline<'d>>;

    fn is_runnable(&self, state: &State<'d, ID, P, Self>) -> bool;

    fn has_io(state: &State<'d, ID, P, Self>) -> bool;
}

impl<'d, ID: Identity, P, X> State<'d, ID, P, X>
where
    P: socket::Prepared,
    X: Mode<'d, ID>,
{
    fn try_new(
        capacity: table::Capacity,
        io: X::Transport<P>,
        timer: timer::Timer<'d>,
    ) -> Result<Self, creation::Error> {
        Ok(Self {
            book: book::Book::try_new(capacity).map_err(creation::Error::Heap)?,
            alarm: timer.try_alarm().map_err(creation::Error::Timer)?,
            io,
        })
    }
}

impl<'d, ID: Identity, P, X> State<'d, ID, P, X>
where
    P: socket::Prepared,
    X: Schedule<'d, ID, P>,
{
    pub(in crate::client) fn rearm(&mut self, target: ready::Target<'d>) {
        let deadline = X::earliest(self);
        self.set(deadline, target);
    }

    pub(super) fn sync(&mut self, before: Option<clock::Deadline<'d>>, target: ready::Target<'d>) {
        let after = X::earliest(self);
        if before != after {
            self.set(after, target);
        }
    }

    fn set(&mut self, deadline: Option<clock::Deadline<'d>>, target: ready::Target<'d>) {
        match deadline {
            Some(deadline) => {
                let wake = wake::Waker::from(target);
                self.alarm.arm(deadline, wake);
            }
            None => self.alarm.clear(),
        }
    }
}

impl<'d, ID: Identity, P> Schedule<'d, ID, P> for side::Disabled
where
    P: socket::Prepared,
{
    fn earliest(state: &State<'d, ID, P, side::Disabled>) -> Option<clock::Deadline<'d>> {
        state.book.earliest()
    }

    fn is_runnable(&self, _: &State<'d, ID, P, Self>) -> bool {
        false
    }

    fn has_io(_: &State<'d, ID, P, Self>) -> bool {
        false
    }
}

impl<'r, 'd, ID: Identity + 'd, P, I> Schedule<'d, ID, P>
    for side::Enabled<&'r registry::Registry<'d, I, ID>>
where
    P: socket::Prepared,
    I: registry::Intent<'d, ID>,
{
    fn earliest(
        state: &State<'d, ID, P, side::Enabled<&'r registry::Registry<'d, I, ID>>>,
    ) -> Option<clock::Deadline<'d>> {
        state
            .book
            .earliest()
            .into_iter()
            .chain(state.io.io.earliest())
            .chain(state.io.retry)
            .min()
    }

    fn is_runnable(&self, state: &State<'d, ID, P, Self>) -> bool {
        let pending = self.registry.pending();
        pending.ready
            || pending.cancel
            || (state.io.retry.is_none() && (pending.deferred || state.io.io.has_retries()))
    }

    fn has_io(state: &State<'d, ID, P, Self>) -> bool {
        state.io.io.has_io()
    }
}

impl<'d, ID: Identity + 'd, P, I> State<'d, ID, P, side::Enabled<&registry::Registry<'d, I, ID>>>
where
    P: socket::Prepared,
    I: registry::Intent<'d, ID>,
{
    pub(in crate::client) fn edit_io<R>(
        &mut self,
        target: ready::Target<'d>,
        edit: impl FnOnce(&mut oneshot::Pool<'d, ID, side::Ticket<'d, ID>, P, I::Payload>) -> R,
    ) -> R {
        let before = Schedule::earliest(self);
        let result = edit(&mut self.io.io);
        self.sync(before, target);
        result
    }

    pub(in crate::client) fn has_retries(&self) -> bool {
        self.io.io.has_retries()
    }

    pub(in crate::client) fn retry_ready(
        &mut self,
        now: clock::Moment<'d>,
        target: ready::Target<'d>,
    ) -> task::Poll<()> {
        let Some(at) = self.io.retry else {
            return task::Poll::Ready(());
        };
        if at > now {
            return task::Poll::Pending;
        }
        let before = Schedule::earliest(self);
        self.io.retry = None;
        self.sync(before, target);
        task::Poll::Ready(())
    }

    pub(in crate::client) fn defer_retry(
        &mut self,
        now: clock::Moment<'d>,
        target: ready::Target<'d>,
    ) {
        if self.io.retry.is_some() {
            return;
        }
        let before = Schedule::earliest(self);
        self.io.retry = Some(self.io.attempts.defer(now));
        self.sync(before, target);
    }

    pub(in crate::client) fn finish_retry(&mut self, deferred: bool, target: ready::Target<'d>) {
        if deferred || self.io.io.has_retries() {
            return;
        }
        self.resume_retry(target);
    }

    pub(in crate::client) fn resume_retry(&mut self, target: ready::Target<'d>) {
        self.io.attempts = super::Attempts::INITIAL;
        if self.io.retry.is_none() {
            return;
        }
        let before = Schedule::earliest(self);
        self.io.retry = None;
        self.sync(before, target);
    }

    pub(in crate::client) fn cancel_next(
        &mut self,
        cursor: &mut u32,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<oneshot::Progress<side::Ticket<'d, ID>>> {
        self.io.io.cancel_next(cursor, driver)
    }

    pub(in crate::client) fn finish(
        &mut self,
        proof: &mut Finish<'_, 'd>,
        release: impl FnMut(side::Ticket<'d, ID>),
    ) {
        self.io.io.finish(proof, release)
    }

    pub(in crate::client) fn expire_one(
        &mut self,
        now: clock::Moment<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<oneshot::Progress<side::Ticket<'d, ID>>> {
        self.io.io.expire_one(now, driver)
    }
}

mod change;
mod egress;
mod links;

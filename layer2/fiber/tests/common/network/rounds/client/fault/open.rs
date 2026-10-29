use std::{mem, pin::Pin, task::Poll};

use dope::{
    manifold::client::conn,
    net::{
        tcp::Tcp,
        wire,
        wire::{
            domain, domain::Limits, failure, open, open::Outcome, receive, receive::mode, send,
        },
    },
};
use dope_core::driver::route::{Identity, KeyTag};
use dope_fiber::{
    abi::{Fiber, Task},
    context,
    context::PollResult,
    net::{
        Io,
        client::{Connect, ConnectError, Machine},
    },
};

#[derive(Debug)]
pub struct Error<'d>(pub &'d str);

#[derive(Clone, Copy)]
pub struct Binding<'d>(pub(crate) &'d str);

impl Default for Binding<'_> {
    fn default() -> Self {
        Self("permanent fiber wire open failure")
    }
}

pub struct Wire;

impl failure::Errors<failure::Open> for Wire {
    type Error<'d>
        = Error<'d>
    where
        Self: 'd;
}

impl wire::Spec for Wire {
    type Connection<'d, ID: Identity + 'd> = ();
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = Binding<'d>;
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Binding<'w>
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Possible;
    type TerminalFailure = failure::Never;

    type Bootstrap = Wire;
    type Receive = receive::Transparent<mode::Disabled>;
    type Send = send::Direct;
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Wire, ID> for Binding<'w> {
    fn open<C, T>(
        &mut self,
        context: C,
        _: impl FnOnce(C, (), ()) -> open::Disposition<T, (), ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Wire>> {
        Outcome::Failed {
            context,
            cause: open::Error::from_cause(Error(self.0)),
        }
    }
}

impl domain::Init<Wire> for Wire {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: Limits,
        bind: <Wire as wire::Spec>::Binding<'w, ID>,
    ) -> <Wire as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Wire: 'w,
        'd: 'w,
    {
        bind
    }
}

pub struct Reuse<'scope, 'd> {
    pub(crate) first: Task<Connect<'scope, 'd, 'd, Tcp, Wire>>,
    pub(crate) second: Task<Connect<'scope, 'd, 'd, Tcp, Wire>>,
    pub(crate) phase: Phase<'d>,
}

pub enum Phase<'d> {
    Start,
    Second,
    First(ConnectError<'d, open::Error<'d, Wire>>),
    Done,
}

type Connected<'scope, 'd> = Result<
    Io<'scope, 'd, 'd, Wire, conn::Id<'d, KeyTag<0>>>,
    ConnectError<'d, open::Error<'d, Wire>>,
>;

pub enum Unexpected<'scope, 'd> {
    First(Poll<Connected<'scope, 'd>>),
    Second(Poll<Connected<'scope, 'd>>),
    Done,
}

impl<'scope, 'd> Fiber<'d> for Reuse<'scope, 'd> {
    type Output = Result<
        (
            ConnectError<'d, open::Error<'d, Wire>>,
            ConnectError<'d, open::Error<'d, Wire>>,
        ),
        Unexpected<'scope, 'd>,
    >;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (self_, mut cx) = call.into_parts();
        let this = self_.get_mut();
        let second = match mem::replace(&mut this.phase, Phase::Done) {
            Phase::Start => {
                return match cx.as_mut().try_poll(Pin::new(&mut this.first)) {
                    PollResult::Exhausted => {
                        this.phase = Phase::Start;
                        Poll::Pending
                    }
                    PollResult::Polled(Poll::Pending) => {
                        this.phase = Phase::Second;
                        Poll::Pending
                    }
                    PollResult::Polled(other) => Poll::Ready(Err(Unexpected::First(other))),
                };
            }
            Phase::Second => match cx.as_mut().try_poll(Pin::new(&mut this.second)) {
                PollResult::Exhausted => {
                    this.phase = Phase::Second;
                    return Poll::Pending;
                }
                PollResult::Polled(Poll::Ready(Err(error))) => error,
                PollResult::Polled(other) => return Poll::Ready(Err(Unexpected::Second(other))),
            },
            Phase::First(error) => error,
            Phase::Done => return Poll::Ready(Err(Unexpected::Done)),
        };
        match cx.as_mut().try_poll(Pin::new(&mut this.first)) {
            PollResult::Exhausted => {
                this.phase = Phase::First(second);
                Poll::Pending
            }
            PollResult::Polled(Poll::Ready(Err(first))) => Poll::Ready(Ok((first, second))),
            PollResult::Polled(other) => Poll::Ready(Err(Unexpected::First(other))),
        }
    }
}

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
pub struct App<'p, 'scope, 'd> {
    #[pin]
    #[manifold]
    pub(crate) connector: Machine<'p, 'scope, 'd, 'd, KeyTag<0>, Tcp, Wire>,
}

use std::{cell::Cell, convert::Infallible, rc::Rc};

use dope_core::driver::{
    route::{Identity, KeyTag},
    settings::{self, claim, errors},
};
use dope_manifold::{
    app::env::Bundle,
    client::session::{self, ingress},
    network::client::topology::{factory::disabled, plan},
    timing::Balanced,
};
use dope_net::{
    tcp::Tcp,
    wire::{
        self, domain, failure, open, receive, receive::mode::Disabled, send, send::reclaim,
        terminal,
    },
};
use dope_test::harness::observe::VecCellExt as _;

use crate::common::{
    Buffer,
    manifold::client::switch::terminal::failure::{Cause, Failure},
};

#[derive(Debug)]
pub enum Observation {
    Prepare(bool),
    PrepareMany(bool),
    Completion { late: bool, failed: bool },
    Settle(bool),
    Flush { failed: bool, empty: bool },
    Drain(bool),
    Close(bool),
}

pub struct State {
    failed: bool,
    observations: Rc<Cell<Vec<Observation>>>,
}

pub struct Outgoing<const LATE: bool> {
    observations: Rc<Cell<Vec<Observation>>>,
}

pub struct Setup {
    pub(crate) app: Failure,
    pub(crate) observations: Rc<Cell<Vec<Observation>>>,
}

impl settings::Resource for Setup {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        Ok(needs)
    }
}

impl<const LATE: bool>
    disabled::Application<(), KeyTag<0>, 1, Bundle<Tcp, Outgoing<LATE>, Balanced>> for Setup
{
    type Input = plan::Local;
    type Session<'life, 'p, 'd>
        = Failure
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd;
    type Source<'life, 'p, 'd>
        = ingress::None
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd;
    type Wire<'life, 'p, 'd>
        = domain::Local<'d, Outgoing<LATE>, KeyTag<0>>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd;
    type Error = Infallible;

    fn build<'life, 'p, 'd>(
        self,
        _: &'life (),
    ) -> Result<
        (
            session::Setup<'d, Self::Session<'life, 'p, 'd>, Self::Source<'life, 'p, 'd>>,
            Self::Wire<'life, 'p, 'd>,
        ),
        Self::Error,
    >
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
    {
        Ok((
            session::Setup::new(self.app, ingress::Plan::none()),
            domain::Local::new(self.observations),
        ))
    }
}

impl<const LATE: bool> failure::Errors<failure::Terminal> for Outgoing<LATE> {
    type Error<'d> = Cause;
}

impl<'w, 'd: 'w, ID: Identity + 'w, const LATE: bool> open::Runtime<'w, 'd, Outgoing<LATE>, ID>
    for Outgoing<LATE>
{
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, State, Buffer<1>) -> open::Disposition<T, State, Buffer<1>>,
    ) -> open::Outcome<C, T, open::Error<'w, Self>> {
        let state = State {
            failed: false,
            observations: Rc::clone(&self.observations),
        };
        let output = match consume(context, state, Buffer::default()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

impl<const LATE: bool> wire::Spec for Outgoing<LATE> {
    type Connection<'d, ID: Identity + 'd> = State;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = Rc<Cell<Vec<Observation>>>;
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Self
    where
        'd: 'w;
    type SendStorage<'d> = Buffer<1>;
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Possible;

    type Bootstrap = Self;
    type Receive = receive::Transparent<Disabled>;
    type Send = Egress;
}

impl<const LATE: bool> domain::Init<Outgoing<LATE>> for Outgoing<LATE> {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: domain::Limits,
        trace: <Self as wire::Spec>::Binding<'w, ID>,
    ) -> <Self as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Self: 'w,
        'd: 'w,
    {
        Self {
            observations: trace,
        }
    }
}

pub struct Egress;

impl<const LATE: bool> send::Policy<Outgoing<LATE>> for Egress {
    type Reclaim = reclaim::OnSubmit;

    fn prepare<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Outgoing<LATE>, send::Storage<'a, Buffer<1>>>,
        plain: send::Plain<'a>,
        end: terminal::Send<'_, terminal::Error<'d, Outgoing<LATE>>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        wire.observations
            .push(Observation::Prepare(plain.is_empty()));
        end.output(send.buffered())
    }

    fn prepare_many<'a, 'd, ID: Identity + 'd, const N: usize>(
        context: send::Context<'a, 'd, ID, Outgoing<LATE>, send::Storage<'a, Buffer<1>>>,
        plain: send::Vectored<'a, N>,
        end: terminal::Send<'_, terminal::Error<'d, Outgoing<LATE>>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        wire.observations
            .push(Observation::PrepareMany(plain.is_empty()));
        end.output(send.buffered())
    }

    fn after_send<'a, 'd, ID: Identity + 'd>(
        context: send::Context<
            'a,
            'd,
            ID,
            Outgoing<LATE>,
            send::Completion<'a, Self::Reclaim, Buffer<1>>,
        >,
        end: terminal::Send<'_, terminal::Error<'d, Outgoing<LATE>>>,
    ) -> terminal::Processed<send::Transition<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, done) = context.into_parts();
        wire.observations.push(Observation::Completion {
            late: LATE,
            failed: wire.failed,
        });
        let send = done.consume_max();
        let complete = send.bytes.is_empty();
        let output = send::Transition::unchanged(send.buffered());
        if complete {
            end.terminal(
                output,
                terminal::Terminal::Failed(terminal::Error::from_cause(Cause::Completion)),
            )
        } else {
            end.output(output)
        }
    }

    fn settle<'a, 'd, ID: Identity + 'd>(
        context: send::Context<
            'a,
            'd,
            ID,
            Outgoing<LATE>,
            send::Completion<'a, Self::Reclaim, Buffer<1>>,
        >,
        _: Option<terminal::Drain<'_, 'd, Outgoing<LATE>>>,
    ) -> send::Transition<'a, Self::Reclaim, impl send::Payload<'a>>
    where
        'd: 'a,
    {
        let (wire, done) = context.into_parts();
        wire.observations.push(Observation::Settle(wire.failed));
        send::Transition::unchanged(done.consume_max().buffered())
    }

    fn flush<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Outgoing<LATE>, send::Storage<'a, Buffer<1>>>,
        end: terminal::Send<'_, terminal::Error<'d, Outgoing<LATE>>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        wire.observations.push(Observation::Flush {
            failed: wire.failed,
            empty: send.bytes.is_empty(),
        });
        end.output(send.buffered())
    }

    fn drain<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Outgoing<LATE>, send::Storage<'a, Buffer<1>>>,
        _: Option<terminal::Drain<'_, 'd, Outgoing<LATE>>>,
    ) -> send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>
    where
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        wire.observations.push(Observation::Drain(wire.failed));
        send.buffered()
    }

    fn close<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Outgoing<LATE>, send::Storage<'a, Buffer<1>>>,
        end: terminal::Send<'_, terminal::Error<'d, Outgoing<LATE>>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, mut send) = context.into_parts();
        wire.observations.push(Observation::Close(wire.failed));
        wire.failed = true;
        if LATE {
            send.bytes = (*b"!").into();
            end.output(send.buffered())
        } else {
            end.terminal(
                send.buffered(),
                terminal::Terminal::Failed(terminal::Error::from_cause(Cause::Close)),
            )
        }
    }
}

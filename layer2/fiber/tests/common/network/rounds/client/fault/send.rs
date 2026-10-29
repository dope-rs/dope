use dope::net::{
    tcp::Tcp,
    wire,
    wire::{
        domain,
        domain::Limits,
        failure, open,
        open::Outcome,
        receive,
        receive::mode,
        send,
        send::{Completion, Payload, Plain, Prepared, Storage, Transition, Vectored, reclaim},
        terminal,
        terminal::Terminal,
    },
};
use dope_core::driver::route::{Identity, KeyTag};
use dope_fiber::net::client::Machine;

use crate::common::network::rounds::client::fault::{Context, Error};

pub struct Wire;

pub struct Conn<'d> {
    pub(crate) failed: bool,
    pub(crate) cause: &'d str,
}

pub struct Policy;

impl send::Policy<Wire> for Policy {
    type Reclaim = reclaim::OnComplete;

    fn prepare<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, Storage<'a, ()>>,
        plain: Plain<'a>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        let fail = !plain.is_empty() && !wire.failed;
        let output = send.input(plain);
        if !fail {
            return end.output(output);
        }
        wire.failed = true;
        let cause = terminal::Error::from_cause(Error::Protocol(wire.cause));
        end.terminal(output, Terminal::Failed(cause))
    }

    fn prepare_many<'a, 'd, ID: Identity + 'd, const N: usize>(
        context: send::Context<'a, 'd, ID, Wire, Storage<'a, ()>>,
        plain: Vectored<'a, N>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        let fail = !plain.is_empty() && !wire.failed;
        let output = send.vectored(plain);
        if !fail {
            return end.output(output);
        }
        wire.failed = true;
        let cause = terminal::Error::from_cause(Error::Protocol(wire.cause));
        end.terminal(output, Terminal::Failed(cause))
    }

    fn after_send<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, Completion<'a, reclaim::OnComplete, ()>>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<Transition<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let done = context.into_state();
        end.output(Transition::completed(done.into_storage()))
    }

    fn settle<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, Completion<'a, reclaim::OnComplete, ()>>,
        _: Option<terminal::Drain<'_, 'd, Wire>>,
    ) -> Transition<'a, Self::Reclaim, impl Payload<'a>>
    where
        'd: 'a,
    {
        let done = context.into_state();
        Transition::completed(done.into_storage())
    }

    fn flush<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, Storage<'a, ()>>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let send = context.into_state();
        end.output(send.empty())
    }

    fn drain<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, Storage<'a, ()>>,
        _: Option<terminal::Drain<'_, 'd, Wire>>,
    ) -> Prepared<'a, Self::Reclaim, impl Payload<'a>>
    where
        'd: 'a,
    {
        let send = context.into_state();
        send.empty()
    }
}

impl failure::Errors<failure::Terminal> for Wire {
    type Error<'d>
        = Error<'d>
    where
        Self: 'd;
}

impl wire::Spec for Wire {
    type Connection<'d, ID: Identity + 'd> = Conn<'d>;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = Context<'d>;
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Context<'w>
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Possible;

    type Bootstrap = Self;
    type Receive = receive::Transparent<mode::Disabled>;
    type Send = Policy;
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Wire, ID> for Context<'w> {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Conn<'w>, ()) -> open::Disposition<T, Conn<'w>, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Wire>> {
        let conn = Conn {
            failed: false,
            cause: self.0,
        };
        let output = match consume(context, conn, ()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        Outcome::Opened(output)
    }
}

impl domain::Init<Wire> for Wire {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: Limits,
        bind: <Self as wire::Spec>::Binding<'w, ID>,
    ) -> <Self as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Self: 'w,
        'd: 'w,
    {
        bind
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

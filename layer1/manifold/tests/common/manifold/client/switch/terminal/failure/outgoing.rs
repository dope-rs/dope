use dope_core::driver::route::Identity;
use dope_net::wire::{
    self, domain, failure, open, receive, receive::mode::Disabled, send, send::reclaim, terminal,
};

use crate::common::{Buffer, manifold::client::switch::terminal::failure::Cause};

pub struct State {
    failed: bool,
}

pub struct Outgoing<const LATE: bool>;

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
        let state = State { failed: false };
        let output = match consume(context, state, Buffer::default()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

impl<const LATE: bool> wire::Spec for Outgoing<LATE> {
    type Connection<'d, ID: Identity + 'd> = State;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = ();
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
        _: <Self as wire::Spec>::Binding<'w, ID>,
    ) -> <Self as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Self: 'w,
        'd: 'w,
    {
        Self
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
        assert!(plain.is_empty(), "unexpected application output");
        let send = context.into_state();
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
        assert!(plain.is_empty(), "unexpected application output");
        let send = context.into_state();
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
        assert!(LATE && wire.failed, "complete outgoing close output");
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
        assert!(wire.failed, "retire outgoing close output");
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
        assert!(
            wire.failed || send.bytes.is_empty(),
            "only close output is buffered"
        );
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
        assert!(wire.failed, "retire outgoing close output");
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
        assert!(!wire.failed, "close wire once");
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

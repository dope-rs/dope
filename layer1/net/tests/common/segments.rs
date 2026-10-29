use std::slice;

use dope_core::{driver::route::Identity, io::transfer};
use dope_net::wire::{self, domain, failure, open, receive, send, terminal};
use o3::base::{marker::ThreadBound, num::bounded};
use receive::mode::Disabled;
use send::reclaim::OnSubmit;

pub struct Wire;

/// Each native submission borrows exactly one byte; the next byte remains owned.
pub struct Segments {
    bytes: [u8; 2],
    consumed: bounded::U32<0, 2>,
    _thread: ThreadBound,
}

impl send::Store for Segments {
    fn as_slice(&self) -> &[u8] {
        self.bytes
            .get(self.consumed.into_usize())
            .map_or(&[], slice::from_ref)
    }

    fn release(self) -> send::Availability {
        send::Availability::Unchanged
    }
}

impl send::Consume for Segments {
    fn consume_max(&mut self, max: transfer::Len) {
        self.consumed = bounded::U32::clamp(self.consumed.into_usize() + max.into_raw());
    }
}

impl send::Policy<Wire> for Wire {
    type Reclaim = OnSubmit;

    fn prepare<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, send::Storage<'a, Segments>>,
        _: send::Plain<'a>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        end.output(context.into_state().buffered())
    }

    fn prepare_many<'a, 'd, ID: Identity + 'd, const N: usize>(
        context: send::Context<'a, 'd, ID, Wire, send::Storage<'a, Segments>>,
        _: send::Vectored<'a, N>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        end.output(context.into_state().buffered())
    }

    fn after_send<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, send::Completion<'a, Self::Reclaim, Segments>>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<send::Transition<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        end.output(send::Transition::unchanged(
            context.into_state().consume_max().buffered(),
        ))
    }

    fn settle<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, send::Completion<'a, Self::Reclaim, Segments>>,
        _: Option<terminal::Drain<'_, 'd, Wire>>,
    ) -> send::Transition<'a, Self::Reclaim, impl send::Payload<'a>>
    where
        'd: 'a,
    {
        let state = context.into_state().consume_max();
        send::Transition::unchanged(state.buffered())
    }

    fn flush<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, send::Storage<'a, Segments>>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        let send = context.into_state().buffered();
        end.output(send)
    }

    fn drain<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, send::Storage<'a, Segments>>,
        _: Option<terminal::Drain<'_, 'd, Wire>>,
    ) -> send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>
    where
        'd: 'a,
    {
        context.into_state().buffered()
    }
}

impl wire::Spec for Wire {
    type Connection<'d, ID: Identity + 'd> = Self;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = ();
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Self
    where
        'd: 'w;
    type SendStorage<'d> = Segments;
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Never;
    type Bootstrap = Self;
    type Receive = receive::Transparent<Disabled>;
    type Send = Self;
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Wire, ID> for Wire {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Wire, Segments) -> open::Disposition<T, Wire, Segments>,
    ) -> open::Outcome<C, T, open::Error<'w, Wire>> {
        let output = match consume(
            context,
            Wire,
            Segments {
                bytes: *b"12",
                consumed: bounded::U32::exact::<0>(),
                _thread: ThreadBound::NEW,
            },
        ) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

impl domain::Init<Wire> for Wire {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: domain::Limits,
        _: <Wire as wire::Spec>::Binding<'w, ID>,
    ) -> <Wire as wire::Spec>::Runtime<'w, 'd, ID>
    where
        'd: 'w,
    {
        Wire
    }
}

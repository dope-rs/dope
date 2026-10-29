use std::rc::Rc;

use dope_core::driver::route::Identity;
use dope_net::wire::{
    self, domain, failure, open, receive, receive::mode, send, send::reclaim, terminal,
};
use dope_test::harness::{
    observe::Trace,
    signals::{Gate, Progress as _},
};
use o3::buffer::write::{Sink as _, consume};

use crate::common::{Buffer, manifold::listener::finish::close::buffer};

pub const BYE: &[u8] = b"<<BYE>>";

pub struct Wire {
    writes: Rc<Trace<consume::Error, 16>>,
    waiting: Option<(Gate, Gate)>,
}

impl Wire {
    pub fn waiting(writes: Rc<Trace<consume::Error, 16>>, entered: Gate, release: Gate) -> Self {
        Self {
            writes,
            waiting: Some((entered, release)),
        }
    }
}

impl From<Rc<Trace<consume::Error, 16>>> for Wire {
    fn from(writes: Rc<Trace<consume::Error, 16>>) -> Self {
        Self {
            writes,
            waiting: None,
        }
    }
}

pub struct Send;

impl send::Policy<Wire> for Send {
    type Reclaim = reclaim::OnSubmit;

    fn has_capacity<'w, 'd: 'w, ID: Identity + 'w>(runtime: &Wire) -> bool
    where
        Wire: 'w,
    {
        runtime
            .waiting
            .as_ref()
            .is_none_or(|(_, release)| release.hits() != 0)
    }

    fn prepare<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, send::Storage<'a, Buffer<16384>>>,
        plain: send::Plain<'a>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        Wire: 'd,
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        end.output(buffer::Buffer::new(send, &wire.writes).plain(plain))
    }

    fn prepare_many<'a, 'd, ID: Identity + 'd, const N: usize>(
        context: send::Context<'a, 'd, ID, Wire, send::Storage<'a, Buffer<16384>>>,
        plain: send::Vectored<'a, N>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        Wire: 'd,
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        end.output(buffer::Buffer::new(send, &wire.writes).vectored(plain))
    }

    fn after_send<'a, 'd, ID: Identity + 'd>(
        context: send::Context<
            'a,
            'd,
            ID,
            Wire,
            send::Completion<'a, reclaim::OnSubmit, Buffer<16384>>,
        >,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<send::Transition<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        Wire: 'd,
        'd: 'a,
    {
        let done = context.into_state();
        end.output(buffer::Buffer::complete(done))
    }

    fn settle<'a, 'd, ID: Identity + 'd>(
        context: send::Context<
            'a,
            'd,
            ID,
            Wire,
            send::Completion<'a, reclaim::OnSubmit, Buffer<16384>>,
        >,
        _: Option<terminal::Drain<'_, 'd, Wire>>,
    ) -> send::Transition<'a, Self::Reclaim, impl send::Payload<'a>>
    where
        Wire: 'd,
        'd: 'a,
    {
        let done = context.into_state();
        buffer::Buffer::complete(done)
    }

    fn flush<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, send::Storage<'a, Buffer<16384>>>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        Wire: 'd,
        'd: 'a,
    {
        let send = context.into_state();
        end.output(send.buffered())
    }

    fn drain<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, send::Storage<'a, Buffer<16384>>>,
        _: Option<terminal::Drain<'_, 'd, Wire>>,
    ) -> send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>
    where
        Wire: 'd,
        'd: 'a,
    {
        let send = context.into_state();
        send.buffered()
    }

    fn close<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, send::Storage<'a, Buffer<16384>>>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        Wire: 'd,
        'd: 'a,
    {
        let (wire, mut send) = context.into_parts();
        if let Some((entered, release)) = &wire.waiting
            && release.hits() == 0
        {
            entered.hit();
            return end.output(send.buffer_wait());
        }
        if let Err(error) = send.bytes.write_slice(BYE) {
            wire.writes.record(error);
        }
        end.output(send.buffered())
    }
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Wire, ID> for Wire {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Wire, Buffer<16384>) -> open::Disposition<T, Wire, Buffer<16384>>,
    ) -> open::Outcome<C, T, open::Error<'w, Wire>> {
        let wire = Wire {
            writes: Rc::clone(&self.writes),
            waiting: self.waiting.clone(),
        };
        let output = match consume(context, wire, Buffer::default()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

impl wire::Spec for Wire {
    type Connection<'d, ID: Identity + 'd>
        = Self
    where
        Self: 'd;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd>
        = Self
    where
        Self: 'd;
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Self
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d>
        = Buffer<16384>
    where
        Self: 'd;
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Never;
    type Bootstrap = Wire;
    type Receive = receive::Transparent<mode::Disabled>;
    type Send = Send;
}

impl domain::Init<Wire> for Wire {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: domain::Limits,
        faults: <Wire as wire::Spec>::Binding<'w, ID>,
    ) -> <Wire as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Wire: 'w,
        'd: 'w,
    {
        faults
    }
}

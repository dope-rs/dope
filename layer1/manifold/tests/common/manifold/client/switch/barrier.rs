use std::{cell::Cell, convert::Infallible, num::NonZero, rc::Rc};

use dope_core::{
    driver::{self, route::KeyTag, schedule::ready},
    io::socket,
};
use dope_manifold::client::{
    app,
    app::{admit::Registered, connect::Full, failure::Observed, request},
    attempt, close, conn, session,
    session::{
        Duplex, Output, Parser, ReceiveMode, Resume, Retirement, Scheduling, Session, Stored,
        Target,
        app::{admit, drain::Ready},
        codec::{self, mode, staging::Limit, storage::Owner},
        policy,
        policy::output,
        response,
        response::work,
        turns,
    },
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    wire,
};
use dope_test::harness::signals::{self, Progress as _};
use o3::{
    base::{Call, cell::region},
    buffer::{self, hold, resident, storage::Shared},
};

use crate::common::{
    contract::Report,
    manifold::client::{
        error::{self, Error::Switch},
        flush,
        switch::{
            Error,
            decode::{self, Decode, Head, State},
        },
    },
};

pub struct Pending(pub(crate) Head);

#[derive(Clone, Copy, Debug)]
pub enum Mode {
    Duplex,
    Yield,
    Blocked,
}

#[derive(Clone)]
pub struct Bytes {
    pub(crate) suffix: Rc<Cell<*const u8>>,
    pub(crate) raw: Rc<Cell<Vec<u8>>>,
}

impl Bytes {
    pub fn snapshot(&self) -> Vec<u8> {
        let raw = self.raw.take();
        let bytes = raw.clone();
        self.raw.set(raw);
        bytes
    }
}

pub struct Gates<'d> {
    pub(crate) target: Rc<Cell<Option<ready::Target<'d>>>>,
    pub(crate) settlements: Rc<Cell<usize>>,
    pub(crate) head: signals::Gate,
    pub(crate) duplex: signals::Gate,
    pub(crate) quarantine: signals::Gate,
    pub(crate) retired: signals::Gate,
}

pub struct Barrier<'d> {
    pub(crate) audit: Rc<Report<error::Error, 16>>,

    pub(crate) decode: Decode,
    pub(crate) attempt: Cell<Option<attempt::Id<'d, KeyTag<0>>>>,
    pub(crate) mode: Rc<Cell<ReceiveMode>>,
    pub(crate) bytes: Bytes,
    pub(crate) gates: Gates<'d>,
    pub(crate) behavior: Mode,
    pub(crate) pending: bool,
}

impl<'d> Session<'d> for Barrier<'d> {
    type Codec = Decode;
    type Connect = Full;
    type Requests = request::Disabled;
    type ConnState = ();
    type Failure = Observed;
    type Send = Shared;

    fn codec(&self) -> &Decode {
        &self.decode
    }

    fn mode(&self, _: conn::Id<'d, KeyTag<0>>, _: &()) -> ReceiveMode {
        self.mode.get()
    }

    fn duplex<'id, 'input, R: buffer::Cursor>(
        &mut self,
        input: codec::Input<'id, 'input, 'd, R>,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> Duplex<'id>
    where
        'd: 'input,
    {
        if input.as_ptr() != self.bytes.suffix.get() {
            self.audit.faults.record(Switch(Error::Suffix {
                actual: input.as_ptr(),
                expected: self.bytes.suffix.get(),
            }));
        }
        let Some(count) = NonZero::new(input.len()) else {
            self.audit.faults.record(Switch(Error::EmptyDuplex));
            return Duplex::Blocked;
        };
        let consumed = match input.try_consumed(count) {
            Ok(consumed) => consumed,
            Err(cause) => return Duplex::Failed(cause),
        };
        let input = input.as_ref();
        let mut raw = self.bytes.raw.take();
        raw.extend_from_slice(input);
        self.bytes.raw.set(raw);
        self.gates.duplex.hit();
        Duplex::Consumed(consumed)
    }
}

impl<'d> policy::Compatible<'d, 'd, wire::Identity, KeyTag<0>> for Barrier<'d> {
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d> Parser<'d> for Barrier<'d> {
    type Mode = mode::Stored;
    type Response = response::Resume<Head, Pending>;
}

impl<'d> Stored<'d> for Barrier<'d> {
    type Delivery = response::Delivery<Head>;

    fn blocked(&self, _: conn::Id<'d, KeyTag<0>>, _: &mut State) -> session::Blocking {
        session::Blocking::Unsupported
    }

    fn decode<S>(
        &mut self,
        _: conn::Id<'d, KeyTag<0>>,
        storage: &mut <Decode as codec::Stateful>::Storage<S>,
        state: &mut State,
        budget: &resident::Budget<'d>,
        limit: Limit,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> codec::Step<Self::Delivery>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        use codec::Stateful as _;
        self.decode.decode(storage, state, budget, limit, |_| {
            codec::Admission::Ready(|result, state: &mut State| self.deliver(result, state))
        })
    }

    fn finish<'slot, 'id, S>(
        &mut self,
        input: Owner<'slot, 'id, <Decode as codec::Stateful>::Storage<S>, State>,
        budget: &resident::Budget<'d>,
        limit: Limit,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> (
        Owner<'slot, 'id, <Decode as codec::Stateful>::Storage<S>, State>,
        Result<codec::Finish<Self::Delivery>, Self::Delivery>,
    )
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        use codec::Stateful as _;
        self.decode.finish(input, budget, limit, |_| {
            codec::Admission::Ready(|result, state: &mut State| self.deliver(result, state))
        })
    }
}

impl Barrier<'_> {
    fn deliver(&self, result: Result<Head, decode::Error>, _state: &mut State) -> Option<Head> {
        let head = match result {
            Ok(head) => head,
            Err(cause) => {
                self.audit.faults.record(error::Error::Decode(cause));
                return None;
            }
        };
        Some(head)
    }
}

impl<'d> Resume<'d, Head, Pending> for Barrier<'d> {
    fn commit<A: work::Admission>(
        &mut self,
        item: response::Committed<Head>,
        _: &mut A,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Settlement<Pending> {
        self.pending = true;
        let pending = Pending(item.into_inner());
        match self.behavior {
            Mode::Duplex | Mode::Yield => response::Settlement::Yield(pending),
            Mode::Blocked => response::Settlement::Blocked(pending),
        }
    }

    fn settle<A: work::Admission>(
        &mut self,
        item: response::Item<Pending>,
        _: &mut A,
        context: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Settlement<Pending> {
        let pending = item.into_inner();
        self.gates.settlements.set(self.gates.settlements.get() + 1);
        match self.behavior {
            Mode::Duplex => {}
            Mode::Yield | Mode::Blocked => {
                let closing = matches!(context.close_status(), conn::CloseStatus::Closing(..));
                let held = self.gates.quarantine.hits();
                if !closing || held != 1 {
                    self.audit.faults.record(Switch(Error::Settlement {
                        closing,
                        quarantines: held,
                    }));
                }
                if self.gates.settlements.get() == 1 {
                    return response::Settlement::Yield(pending);
                }
            }
        }
        self.pending = false;
        self.bytes.suffix.set(pending.0.suffix);
        self.mode.set(ReceiveMode::Paused);
        self.gates.head.hit();
        response::Settlement::Ready
    }
}

impl<'d> Output<'d> for Barrier<'d> {
    fn flush() -> impl for<'a, 'c, 't, 'e> Call<(
        &'a mut Self,
        &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send>,
    )> {
        flush(|owner: &mut Self, context| {
            if owner.attempt.get().is_none() {
                owner.audit.faults.record(Switch(Error::Unregistered));
            }
            if !context.sink.is_empty() {
                owner.audit.faults.record(Switch(Error::PendingOutput));
            }
            if owner.pending && matches!(owner.behavior, Mode::Yield | Mode::Blocked) {
                context.request(close::Directive::cut_final(close::Reason::Protocol));
            }
        })
    }

    fn sent() -> impl for<'a, 't> Call<(
        &'a mut Self,
        conn::Id<'d, KeyTag<0>>,
        egress::Pending,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Sent::<_>::Ready(|owner: &mut Self| {
            owner.audit.record_sent();
        })
    }

    fn cancel() -> impl for<'a, 't> Call<(
        &'a mut Self,
        Self::Send,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        use Infallible;

        output::Cancel::<Infallible, Infallible, Infallible, _, _>::Ready(
            drop::<Self::Send>,
            |owner: &mut Self| {
                owner.audit.cancelled();
            },
        )
    }
}

impl<'d> admit::Register<'d> for Barrier<'d> {
    fn register(&self, active: &conn::Activation<'_, 'd, KeyTag<0>>) -> app::Admission {
        self.attempt.set(Some(active.attempt()));
        self.gates.target.set(Some(active.ready()));
        app::Admission::Attached
    }
}

impl<'d> session::Connected<'d> for Barrier<'d> {
    type Activation = Registered;

    fn connect(
        &mut self,
        attempt: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        ctx: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        if self.attempt.get() != Some(attempt.id()) {
            self.audit.faults.record(Switch(Error::AttemptMismatch));
        }
        if self.gates.target.get() != Some(ctx.ready) {
            self.audit.faults.record(Switch(Error::TargetMismatch));
        }
    }
}

impl<'d> session::Failed<'d> for Barrier<'d> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d> session::Exhausted<'d> for Barrier<'d> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d> Retirement<'d> for Barrier<'d> {
    type Drain = Ready;

    fn io_fail(
        &mut self,
        error: dope_core::Errno,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        self.audit.faults.record(error::Error::Receive(error));
    }

    fn quarantine(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut (),
        _: close::Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let framed = self.mode.get() == ReceiveMode::Framed;
        let duplex = self.mode.get() == ReceiveMode::Duplex;
        if match self.behavior {
            Mode::Duplex => !duplex,
            Mode::Yield | Mode::Blocked => !framed || !self.pending,
        } {
            self.audit.faults.record(Switch(Error::Mode {
                actual: self.behavior,
                framed,
                duplex,
                pending: self.pending,
            }));
        }
        self.gates.quarantine.hit();
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn disconnect(&mut self, _: turns::Retired<'_, 'd, Self::ConnState, KeyTag<0>, Self>) {
        if self.attempt.get().is_none() {
            self.audit.faults.record(Switch(Error::Unregistered));
        }
        let bytes = self.bytes.snapshot();
        let settled = self.gates.settlements.get();
        if match self.behavior {
            Mode::Duplex => bytes.as_slice() != b"RAW",
            Mode::Yield | Mode::Blocked => !bytes.is_empty() || settled != 2,
        } {
            self.audit.faults.record(Switch(Error::Retired {
                bytes,
                settlements: settled,
            }));
        }
        self.gates.retired.hit();
    }
}

impl<'d> Scheduling<'d> for Barrier<'d> {}
impl<'d> Target<'d, KeyTag<0>, 1> for Barrier<'d> {}

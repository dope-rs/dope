use std::{cell::Cell, convert::Infallible, rc::Rc};

use dope_core::{
    driver::{self, route::KeyTag, schedule::ready},
    io::socket,
};
use dope_manifold::client::{
    app,
    app::{admit::Registered, connect::Full, failure::Observed, request},
    attempt, close, conn, session,
    session::{
        Output, Parser, ReceiveMode, Retirement, Scheduling, Session, Stored, Target,
        app::{admit, drain::Ready},
        codec::{self, mode, staging::Limit, storage::Owner},
        policy,
        policy::output,
        response, turns,
    },
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    wire::{self, failure},
};
use dope_test::harness::signals;
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
            terminal::queued,
        },
    },
};

pub struct Retry<'d> {
    pub(crate) audit: Rc<Report<error::Error, 16>>,

    pub(crate) decode: Decode,
    pub(crate) target: Rc<Cell<Option<ready::Target<'d>>>>,
    pub(crate) suffix: Rc<Cell<*const u8>>,
    pub(crate) tries: Rc<Cell<usize>>,
    pub(crate) released: Rc<Cell<bool>>,
    pub(crate) closing: bool,
    pub(crate) stages: Stages,
}

pub struct Stages {
    pub(crate) blocked: signals::Gate,
    pub(crate) commit: signals::Gate,
    pub(crate) cleared: signals::Gate,
    pub(crate) retired: Option<signals::Gate>,
    pub(crate) terminal: Option<Rc<queued::State>>,
}

impl Retry<'_> {
    fn check(&self) {
        let tried = self.tries.get();
        let limit = if self.closing {
            1
        } else {
            self.decode.attempts.get()
        };
        if tried != limit {
            self.audit.faults.record(Switch(Error::Attempts {
                actual: tried,
                limit,
            }));
        }
    }
}

impl<'d> Session<'d> for Retry<'d> {
    type Codec = Decode;
    type Connect = Full;
    type ConnState = ();
    type Failure = Observed;
    type Requests = request::Disabled;
    type Send = Shared;
    fn codec(&self) -> &Decode {
        &self.decode
    }

    fn mode(&self, _: conn::Id<'d, KeyTag<0>>, _: &()) -> ReceiveMode {
        if self.tries.get() != 0 && !self.released.get() {
            ReceiveMode::Paused
        } else if self.stages.terminal.is_some() && self.tries.get() == self.decode.attempts.get() {
            ReceiveMode::Duplex
        } else {
            ReceiveMode::Framed
        }
    }

    fn eof<'e>(
        &mut self,
        _: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send>,
    ) -> app::Eof {
        match &self.stages.terminal {
            Some(terminal) => {
                terminal.events.record(queued::Event::Eof);
                app::Eof::Retain
            }
            None => app::Eof::Close,
        }
    }
}

impl<'d, W> policy::Compatible<'d, 'd, W, KeyTag<0>> for Retry<'d>
where
    W: wire::Spec<OpenFailure = failure::Never, TerminalFailure = failure::Never> + 'd,
{
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d> Parser<'d> for Retry<'d> {
    type Mode = mode::Stored;
    type Response = response::Immediate;
}

impl<'d> Stored<'d> for Retry<'d> {
    type Delivery = response::Direct;

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
        context: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> codec::Step<Self::Delivery>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        use codec::Stateful as _;
        self.decode
            .decode(storage, state, budget, limit, |preview| {
                let admit = self.admit(preview);
                if self.closing && matches!(&admit, codec::Admission::RetryInput) {
                    context.request(close::Directive::cut_final(close::Reason::Protocol));
                }
                admit
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
        self.decode
            .finish(input, budget, limit, |preview| self.admit(preview))
    }
}

impl<'d> Retry<'d> {
    fn admit<'a>(
        &'a self,
        preview: Result<&Head, &decode::Error>,
    ) -> codec::Admission<impl FnOnce(Result<Head, decode::Error>, &mut State) + use<'a, 'd>> {
        if let Ok(head) = preview {
            let attempt = self.tries.get();
            self.tries.set(attempt + 1);
            if attempt == 0 {
                self.suffix.set(head.suffix);
                self.stages.blocked.hit();
                return codec::Admission::RetryInput;
            }
            if attempt == 1 && head.suffix != self.suffix.get() {
                self.audit.faults.record(Switch(Error::Suffix {
                    actual: head.suffix,
                    expected: self.suffix.get(),
                }));
            }
        }
        codec::Admission::Ready(
            |result: Result<Head, decode::Error>, _: &mut State| match result {
                Ok(_) => {
                    self.stages.commit.hit();
                    if let Some(terminal) = &self.stages.terminal {
                        terminal.events.record(queued::Event::Input);
                    }
                }
                Err(cause) => {
                    self.audit.faults.record(error::Error::Decode(cause));
                }
            },
        )
    }
}

impl<'d> Output<'d> for Retry<'d> {
    fn flush() -> impl for<'a, 'c, 't, 'e> Call<(
        &'a mut Self,
        &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send>,
    )> {
        flush(|owner: &mut Self, context| {
            if owner.target.get().is_none() {
                owner.audit.faults.record(Switch(Error::Unregistered));
            }
            if !context.sink.is_empty() {
                owner.audit.faults.record(Switch(Error::PendingOutput));
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

impl<'d> admit::Register<'d> for Retry<'d> {
    fn register(&self, active: &conn::Activation<'_, 'd, KeyTag<0>>) -> app::Admission {
        self.target.set(Some(active.ready()));
        app::Admission::Attached
    }
}

impl<'d> session::Connected<'d> for Retry<'d> {
    type Activation = Registered;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        ctx: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        if self.target.get() != Some(ctx.ready) {
            self.audit.faults.record(Switch(Error::TargetMismatch));
        }
    }
}

impl<'d> session::Failed<'d> for Retry<'d> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        cause: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(cause));
    }
}

impl<'d> session::Exhausted<'d> for Retry<'d> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(cause));
    }
}

impl<'d> Retirement<'d> for Retry<'d> {
    type Drain = Ready;

    fn io_fail(
        &mut self,
        cause: dope_core::Errno,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        if let Some(terminal) = &self.stages.terminal {
            terminal.events.record(queued::Event::Failed(cause));
        } else {
            self.audit.faults.record(error::Error::Receive(cause));
        }
    }

    fn quarantine(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut (),
        _reason: close::Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        self.check();
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        self.stages.cleared.hit();
        egress::Cleared::Done
    }

    fn fence(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &Self::ConnState,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> app::Fence {
        if self.stages.terminal.is_some() {
            app::Fence::Pending
        } else {
            app::Fence::Ready
        }
    }

    fn disconnect(&mut self, ctx: turns::Retired<'_, 'd, Self::ConnState, KeyTag<0>, Self>) {
        self.check();
        if let Some(retired) = &self.stages.retired {
            let reason = if self.closing {
                close::Reason::Protocol
            } else if let Some(terminal) = &self.stages.terminal {
                terminal.events.record(queued::Event::Closed(ctx.reason()));
                close::Reason::Transport
            } else {
                close::Reason::Remote
            };
            if ctx.reason() != reason {
                self.audit.faults.record(Switch(Error::Close(ctx.reason())));
            }
            retired.hit();
        }
    }
}

impl<'d> Scheduling<'d> for Retry<'d> {}
impl<'d> Target<'d, KeyTag<0>, 1> for Retry<'d> {}

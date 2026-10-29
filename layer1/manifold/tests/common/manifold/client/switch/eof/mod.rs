use std::{cell::Cell, convert::Infallible, rc::Rc};

use dope_core::{
    driver::{
        self,
        route::KeyTag,
        schedule::ready,
        settings,
        settings::{claim, errors},
    },
    io::socket,
};
use dope_manifold::{
    app::env::Bundle,
    client::{
        app,
        app::{admit::Registered, connect::Full, failure::Observed, request},
        attempt, close, conn, session,
        session::{
            Output, Parser, Resume, Retirement, Scheduling, Session, Stored, Target,
            app::{admit, drain::Ready},
            codec::{self, mode, staging::Limit, storage},
            policy,
            policy::output,
            response,
            response::work,
            turns,
        },
    },
    network::client::topology::{factory::disabled, plan},
    timing::Balanced,
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    tcp::Tcp,
    wire::{self, domain},
};
use dope_test::harness::signals;
use o3::{
    base::{Call, cell::region},
    buffer::{self, hold, resident, storage::Shared},
};
use session::ingress;

use crate::common::{
    contract::Report,
    manifold::client::{
        error::{self, Error::Switch},
        flush,
        switch::Error,
    },
};

pub mod decode;
use decode::{Decode, Head, ParseState};

pub struct Pending;

pub struct Stages {
    pub(crate) blocked: signals::Gate,
    pub(crate) commit: signals::Gate,
    pub(crate) settled: signals::Gate,
    pub(crate) closed: signals::Gate,
}

pub struct State<'d> {
    pub(crate) audit: Rc<Report<error::Error, 16>>,

    pub(crate) decode: Decode,
    pub(crate) target: Cell<Option<ready::Target<'d>>>,
    pub(crate) phase: Rc<Cell<u8>>,
    pub(crate) stages: Stages,
}

impl<'d> Session<'d> for State<'d> {
    type Codec = Decode;
    type Connect = Full;
    type ConnState = ();
    type Failure = Observed;
    type Send = Shared;
    type Requests = request::Disabled;

    fn codec(&self) -> &Decode {
        &self.decode
    }
}

impl<'d> policy::Compatible<'d, 'd, wire::Identity, KeyTag<0>> for State<'d> {
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d> Parser<'d> for State<'d> {
    type Mode = mode::Stored;
    type Response = response::Resume<Head, Pending>;
}

impl<'d> Stored<'d> for State<'d> {
    type Delivery = response::Delivery<Head>;

    fn blocked(&self, _: conn::Id<'d, KeyTag<0>>, _: &mut ParseState) -> session::Blocking {
        session::Blocking::Unsupported
    }

    fn decode<S>(
        &mut self,
        _: conn::Id<'d, KeyTag<0>>,
        storage: &mut <Decode as codec::Stateful>::Storage<S>,
        state: &mut ParseState,
        budget: &resident::Budget<'d>,
        limit: Limit,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> codec::Step<Self::Delivery>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        use codec::Stateful as _;
        self.decode
            .decode(storage, state, budget, limit, |preview| self.admit(preview))
    }

    fn finish<'slot, 'id, S>(
        &mut self,
        input: storage::Owner<'slot, 'id, <Decode as codec::Stateful>::Storage<S>, ParseState>,
        budget: &resident::Budget<'d>,
        limit: Limit,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> (
        storage::Owner<'slot, 'id, <Decode as codec::Stateful>::Storage<S>, ParseState>,
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

impl<'d> State<'d> {
    fn admit<'a>(
        &'a self,
        preview: Result<&Head, &Error>,
    ) -> codec::Admission<
        impl FnOnce(Result<Head, Error>, &mut ParseState) -> Option<Head> + use<'a, 'd>,
    > {
        let mut accepted = true;
        if preview.is_ok() && self.phase.get() == 0 {
            self.phase.set(1);
            self.stages.blocked.hit();
            if let Some(target) = self.target.get() {
                target.wake();
                return codec::Admission::RetryInput;
            }
            self.audit.faults.record(Switch(Error::Unregistered));
            accepted = false;
        }
        codec::Admission::Ready(move |result: Result<Head, Error>, state: &mut ParseState| {
            if accepted {
                self.deliver(result, state)
            } else {
                None
            }
        })
    }

    fn deliver(&self, result: Result<Head, Error>, _state: &mut ParseState) -> Option<Head> {
        let head = match result {
            Ok(head) => head,
            Err(cause) => {
                self.audit.faults.record(Switch(cause));
                return None;
            }
        };
        let phase = self.phase.get();
        if phase != 1 {
            self.audit.faults.record(Switch(Error::Phase {
                actual: phase,
                expected: 1,
            }));
            return None;
        }
        self.phase.set(2);
        Some(head)
    }
}

impl<'d> Resume<'d, Head, Pending> for State<'d> {
    fn commit<A: work::Admission>(
        &mut self,
        item: response::Committed<Head>,
        _: &mut A,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Settlement<Pending> {
        let Head = item.into_inner();
        let actual = self.phase.replace(3);
        if actual != 2 {
            self.audit.faults.record(Switch(Error::Phase {
                actual,
                expected: 2,
            }));
        }
        self.stages.commit.hit();
        response::Settlement::Yield(Pending)
    }

    fn settle<A: work::Admission>(
        &mut self,
        _: response::Item<Pending>,
        _: &mut A,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Settlement<Pending> {
        let actual = self.phase.replace(4);
        if actual != 3 {
            self.audit.faults.record(Switch(Error::Phase {
                actual,
                expected: 3,
            }));
        }
        self.stages.settled.hit();
        response::Settlement::Ready
    }
}

impl<'d> Output<'d> for State<'d> {
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

impl<'d> admit::Register<'d> for State<'d> {
    fn register(&self, active: &conn::Activation<'_, 'd, KeyTag<0>>) -> app::Admission {
        self.target.set(Some(active.ready()));
        app::Admission::Attached
    }
}

impl<'d> session::Connected<'d> for State<'d> {
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

impl<'d> session::Failed<'d> for State<'d> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d> session::Exhausted<'d> for State<'d> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d> Retirement<'d> for State<'d> {
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
        let actual = self.phase.get();
        if actual != 0 {
            self.audit.faults.record(Switch(Error::Phase {
                actual,
                expected: 0,
            }));
        }
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn disconnect(&mut self, _: turns::Retired<'_, 'd, Self::ConnState, KeyTag<0>, Self>) {
        let actual = self.phase.get();
        if actual != 4 {
            self.audit.faults.record(Switch(Error::Phase {
                actual,
                expected: 4,
            }));
        }
        self.stages.closed.hit();
    }
}

impl<'d> Scheduling<'d> for State<'d> {}
impl<'d> Target<'d, KeyTag<0>, 1> for State<'d> {}

pub struct Eof {
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) decode: Decode,
    pub(crate) phase: Rc<Cell<u8>>,
    pub(crate) stages: Stages,
}

impl settings::Resource for Eof {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        Ok(needs)
    }
}

impl disabled::Application<(), KeyTag<0>, 1, Bundle<Tcp, wire::Identity, Balanced>> for Eof {
    type Input = plan::Local;
    type Session<'life, 'p, 'd>
        = State<'d>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        (): 'd,
        wire::Identity: 'd;
    type Source<'life, 'p, 'd>
        = ingress::None
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        (): 'd,
        wire::Identity: 'd;
    type Wire<'life, 'p, 'd>
        = domain::Local<'d, wire::Identity, KeyTag<0>>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        (): 'd,
        wire::Identity: 'd;
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
        (): 'd,
        wire::Identity: 'd,
    {
        let state = State {
            audit: self.audit,

            decode: self.decode,
            target: Cell::new(None),
            phase: self.phase,
            stages: self.stages,
        };
        Ok((
            session::Setup::new(state, ingress::Plan::none()),
            domain::Local::new(wire::Identity),
        ))
    }
}

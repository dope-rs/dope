use std::{cell::Cell, convert::Infallible, rc::Rc};

use dope_core::{driver, driver::route::KeyTag, io::socket};
use dope_manifold::client::{
    app::{admit::Automatic, connect::Full, failure::Observed, request},
    attempt, close, conn,
    session::{
        self, Output, Parser, Resume, Retirement, Scheduling, Session, Stored, Target,
        app::drain::Ready,
        codec::{
            self, Codec, Parse, Stateful, mode,
            staging::{Buffered, Limit},
            storage,
        },
        policy::{self, output},
        response,
        response::work,
        turns,
    },
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    wire::{self, failure},
};
use dope_test::harness::signals;
use o3::{
    base::{Call, cell::region},
    buffer,
    buffer::{
        hold,
        resident::{self, staging},
        storage::Shared,
    },
};

use crate::common::{
    contract::Report,
    manifold::client::{
        error::{self, Error::Switch},
        flush, switch,
    },
};

pub struct Decode;

pub enum Fault {
    Rejected,
    Invalid(switch::Error),
}

pub struct Prepared;

pub struct Pending;

pub struct ParseState {
    pub(crate) rejected: bool,
}

impl Codec for Decode {
    type Head<'input, 'd, R: buffer::Cursor>
        = Infallible
    where
        R: 'input,
        'd: 'input;
    type Error = Fault;
    type Staging = Buffered;

    fn staging(&self) -> Result<staging::Plan, staging::Error> {
        staging::Plan::new(1, 1)
    }
}

impl Stateful for Decode {
    type State<'d> = ParseState;

    fn state<'d>(&self, _: resident::Budget<'d>) -> ParseState {
        ParseState { rejected: false }
    }

    type Storage<S> = S;
    type Preview<'a> = &'a Infallible;
    type Outcome<'d> = Infallible;

    fn storage<'d, S>(&self, source: S, _: &ParseState) -> S {
        source
    }

    fn source<S: buffer::raw::Source>(storage: &S) -> &S {
        storage
    }

    fn edit<S>(storage: &mut S) -> storage::Edit<'_, S> {
        storage::Edit::new(storage)
    }

    fn restart<'a, 'd, S>(&self, storage: &'a mut S, _: &ParseState) -> storage::Edit<'a, S> {
        Self::edit(storage)
    }

    fn decode<'d, S, T, F>(
        &self,
        storage: &mut S,
        state: &mut ParseState,
        budget: &resident::Budget<'d>,
        limit: Limit,
        admit: impl FnOnce(Result<Self::Preview<'_>, &Self::Error>) -> codec::Admission<F>,
    ) -> codec::Step<T>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
        T: response::Outcome,
        F: FnOnce(Result<Self::Outcome<'d>, Self::Error>, &mut ParseState) -> T::Accepted,
    {
        codec::Input::commit(storage, budget, limit, |input| {
            switch::parsed(self.parse_once(state, input), state, admit)
        })
    }

    fn finish<'slot, 'id, 'd, S, T, F>(
        &self,
        mut input: storage::Owner<'slot, 'id, S, ParseState>,
        budget: &resident::Budget<'d>,
        limit: Limit,
        admit: impl FnOnce(Result<Self::Preview<'_>, &Self::Error>) -> codec::Admission<F>,
    ) -> (
        storage::Owner<'slot, 'id, S, ParseState>,
        Result<codec::Finish<T>, T>,
    )
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
        T: response::Outcome,
        F: FnOnce(Result<Self::Outcome<'d>, Self::Error>, &mut ParseState) -> T::Accepted,
    {
        let (storage, state) = input.parts();
        let result = codec::Input::with(storage, budget, limit, |input| {
            switch::finished(self.finish_once(state, input), state, admit)
        });
        (input, result)
    }
}

impl Decode {
    fn parse_once<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        state: &mut ParseState,
        _: codec::Input<'id, 'input, 'd, R>,
    ) -> Result<Parse<'id, Infallible>, Fault>
    where
        'd: 'input,
    {
        if state.rejected {
            return Err(Fault::Invalid(switch::Error::RepeatedReject));
        }
        state.rejected = true;
        Err(Fault::Rejected)
    }

    fn finish_once<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        state: &mut ParseState,
        _: codec::Input<'id, 'input, 'd, R>,
    ) -> Result<codec::Finish<Infallible>, Fault>
    where
        'd: 'input,
    {
        if state.rejected {
            return Err(Fault::Invalid(switch::Error::RejectedEof));
        }
        Ok(codec::Finish::Empty)
    }
}

pub struct Reject {
    pub(crate) audit: Rc<Report<error::Error, 16>>,

    pub(crate) decode: Decode,
    pub(crate) phase: Rc<Cell<u8>>,
    pub(crate) commit: signals::Gate,
    pub(crate) settled: signals::Gate,
    pub(crate) closed: signals::Gate,
}

impl<'d> Session<'d> for Reject {
    type Codec = Decode;
    type Connect = Full;
    type ConnState = ();
    type Failure = Observed;
    type Requests = request::Disabled;
    type Send = Shared;

    fn codec(&self) -> &Decode {
        &self.decode
    }
}

impl<'d, W> policy::Compatible<'d, 'd, W, KeyTag<0>> for Reject
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

impl<'d> Parser<'d> for Reject {
    type Mode = mode::Stored;
    type Response = response::Resume<Prepared, Pending>;
}

impl<'d> Stored<'d> for Reject {
    type Delivery = response::Delivery<Prepared>;

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
        self.decode.decode(storage, state, budget, limit, |_| {
            codec::Admission::Ready(|result, state: &mut ParseState| self.deliver(result, state))
        })
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
        self.decode.finish(input, budget, limit, |_| {
            codec::Admission::Ready(|result, state: &mut ParseState| self.deliver(result, state))
        })
    }
}

impl Reject {
    fn deliver(
        &self,
        result: Result<Infallible, Fault>,
        state: &mut ParseState,
    ) -> Option<Prepared> {
        match result {
            Ok(head) => match head {},
            Err(Fault::Invalid(cause)) => {
                self.audit.faults.record(Switch(cause));
                None
            }
            Err(Fault::Rejected) => {
                if !state.rejected {
                    self.audit
                        .faults
                        .record(Switch(switch::Error::RejectedState(state.rejected)));
                }
                let actual = self.phase.replace(1);
                if actual != 0 {
                    self.audit.faults.record(Switch(switch::Error::Phase {
                        actual,
                        expected: 0,
                    }));
                }
                Some(Prepared)
            }
        }
    }
}

impl<'d> Resume<'d, Prepared, Pending> for Reject {
    fn commit<A: work::Admission>(
        &mut self,
        item: response::Committed<Prepared>,
        _: &mut A,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Settlement<Pending> {
        let Prepared = item.into_inner();
        let actual = self.phase.replace(2);
        if actual != 1 {
            self.audit.faults.record(Switch(switch::Error::Phase {
                actual,
                expected: 1,
            }));
        }
        self.commit.hit();
        response::Settlement::Yield(Pending)
    }

    fn settle<A: work::Admission>(
        &mut self,
        _: response::Item<Pending>,
        _: &mut A,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Settlement<Pending> {
        let actual = self.phase.replace(3);
        if actual != 2 {
            self.audit.faults.record(Switch(switch::Error::Phase {
                actual,
                expected: 2,
            }));
        }
        self.settled.hit();
        response::Settlement::Ready
    }
}

impl<'d> Output<'d> for Reject {
    fn flush() -> impl for<'a, 'c, 't, 'e> Call<(
        &'a mut Self,
        &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send>,
    )> {
        flush(|owner: &mut Self, context| {
            if !context.sink.is_empty() {
                owner
                    .audit
                    .faults
                    .record(Switch(switch::Error::PendingOutput));
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
        use std::convert::Infallible;

        output::Cancel::<Infallible, Infallible, Infallible, _, _>::Ready(
            drop::<Self::Send>,
            |owner: &mut Self| {
                owner.audit.cancelled();
            },
        )
    }
}

impl<'d> session::Connected<'d> for Reject {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        _: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        let actual = self.phase.get();
        if actual != 0 {
            self.audit.faults.record(Switch(switch::Error::Phase {
                actual,
                expected: 0,
            }));
        }
    }
}

impl<'d> session::Failed<'d> for Reject {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d> session::Exhausted<'d> for Reject {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d> Retirement<'d> for Reject {
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
        if actual != 3 {
            self.audit.faults.record(Switch(switch::Error::Phase {
                actual,
                expected: 3,
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

    fn disconnect(&mut self, context: turns::Retired<'_, 'd, Self::ConnState, KeyTag<0>, Self>) {
        if context.reason() != close::Reason::Protocol {
            self.audit
                .faults
                .record(Switch(switch::Error::Close(context.reason())));
        }
        let actual = self.phase.get();
        if actual != 3 {
            self.audit.faults.record(Switch(switch::Error::Phase {
                actual,
                expected: 3,
            }));
        }
        self.closed.hit();
    }
}

impl<'d> Scheduling<'d> for Reject {}
impl<'d> Target<'d, KeyTag<0>, 1> for Reject {}

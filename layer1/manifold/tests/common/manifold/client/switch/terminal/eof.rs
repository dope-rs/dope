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
    wire,
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

pub struct Head;

pub struct Pending;

pub struct ParseState {
    pub(crate) parsed: bool,
}

impl Codec for Decode {
    type Head<'input, 'd, R: buffer::Cursor>
        = Head
    where
        R: 'input,
        'd: 'input;
    type Error = switch::Error;
    type Staging = Buffered;

    fn staging(&self) -> Result<staging::Plan, staging::Error> {
        staging::Plan::new(1, 2)
    }
}

impl Stateful for Decode {
    type State<'d> = ParseState;

    fn state<'d>(&self, _: resident::Budget<'d>) -> ParseState {
        ParseState { parsed: false }
    }

    type Storage<S> = S;
    type Preview<'a> = &'a Head;
    type Outcome<'d> = Head;

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
        input: codec::Input<'id, 'input, 'd, R>,
    ) -> Result<Parse<'id, Head>, switch::Error>
    where
        'd: 'input,
    {
        if state.parsed {
            return Err(switch::Error::RepeatedParse);
        }
        state.parsed = true;
        Ok(match input.need_more(None) {
            Some(need) => Parse::More(need),
            None => Parse::CapacityExhausted,
        })
    }

    fn finish_once<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        state: &mut ParseState,
        input: codec::Input<'id, 'input, 'd, R>,
    ) -> Result<codec::Finish<Head>, switch::Error>
    where
        'd: 'input,
    {
        if !state.parsed {
            return Err(switch::Error::UnparsedEof);
        }
        if input.as_slice() != b"T" {
            return Err(switch::Error::TerminalBytes(input.as_slice().to_vec()));
        }
        Ok(codec::Finish::Item(Head))
    }
}

pub struct Eof {
    pub(crate) audit: Rc<Report<error::Error, 16>>,

    pub(crate) decode: Decode,
    pub(crate) phase: Rc<Cell<u8>>,
    pub(crate) commit: signals::Gate,
    pub(crate) settled: signals::Gate,
    pub(crate) closed: signals::Gate,
}

impl Eof {
    fn check(&self) {
        let actual = self.phase.get();
        if actual != 0 {
            self.audit.faults.record(Switch(switch::Error::Phase {
                actual,
                expected: 0,
            }));
        }
    }
}

impl<'d> Session<'d> for Eof {
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

impl<'d> policy::Compatible<'d, 'd, wire::Identity, KeyTag<0>> for Eof {
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d> Parser<'d> for Eof {
    type Mode = mode::Stored;
    type Response = response::Resume<Head, Pending>;
}

impl<'d> Stored<'d> for Eof {
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

impl Eof {
    fn deliver(
        &self,
        result: Result<Head, switch::Error>,
        _state: &mut ParseState,
    ) -> Option<Head> {
        let head = match result {
            Ok(head) => head,
            Err(cause) => {
                self.audit.faults.record(Switch(cause));
                return None;
            }
        };
        let actual = self.phase.replace(1);
        if actual != 0 {
            self.audit.faults.record(Switch(switch::Error::Phase {
                actual,
                expected: 0,
            }));
        }
        Some(head)
    }
}

impl<'d> Resume<'d, Head, Pending> for Eof {
    fn commit<A: work::Admission>(
        &mut self,
        item: response::Committed<Head>,
        _: &mut A,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Settlement<Pending> {
        let Head = item.into_inner();
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

impl<'d> Output<'d> for Eof {
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
        use Infallible;

        output::Cancel::<Infallible, Infallible, Infallible, _, _>::Ready(
            drop::<Self::Send>,
            |owner: &mut Self| {
                owner.audit.cancelled();
            },
        )
    }
}

impl<'d> session::Connected<'d> for Eof {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        _: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        self.check();
    }
}

impl<'d> session::Failed<'d> for Eof {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d> session::Exhausted<'d> for Eof {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d> Retirement<'d> for Eof {
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
        self.check();
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
        if actual != 3 {
            self.audit.faults.record(Switch(switch::Error::Phase {
                actual,
                expected: 3,
            }));
        }
        self.closed.hit();
    }
}

impl<'d> Scheduling<'d> for Eof {}
impl<'d> Target<'d, KeyTag<0>, 1> for Eof {}

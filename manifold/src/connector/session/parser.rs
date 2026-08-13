use std::ops;

use dope_core::driver::schedule;
use dope_net::wire;
use o3::buffer::resident;

use crate::connector::{codec, lifecycle, session};

pub(super) enum Outcome {
    Complete,
    Yield,
    Close,
    Capacity,
    Overrun,
}

enum PreserveError {
    Capacity,
    Contract,
}

enum Step {
    NeedMore,
    Capacity,
    Close,
    Item(usize),
}

enum AdmittedReady<'turn, 'd> {
    Available(schedule::ApplicationPermit<'turn, 'd>),
    Spent,
    Outcome(Outcome),
}

pub(super) struct Parser<'a, 'work, 'connection, 'd, const ID: u8, N: session::Session<'d, ID>> {
    owner: &'a mut N,
    ingress: &'a mut resident::Snapshot<'d, { session::INGRESS_BUF_CAP }>,
    budget: &'a resident::Budget<'d>,
    parse_state: &'a mut <N::Codec as codec::Codec>::ParseState,
    context: &'a mut session::Ctx<'connection, 'd, N, ID>,
    work: schedule::Application<'work, 'd>,
}

struct Core<'a, 'work, 'connection, 'd, const ID: u8, N: session::Session<'d, ID>> {
    owner: &'a mut N,
    budget: &'a resident::Budget<'d>,
    parse_state: &'a mut <N::Codec as codec::Codec>::ParseState,
    context: &'a mut session::Ctx<'connection, 'd, N, ID>,
    work: schedule::Application<'work, 'd>,
}

struct Direct<'a, 'd, R: wire::Cursor<'d>> {
    cursor: &'a mut R,
    ingress: &'a mut resident::Snapshot<'d, { session::INGRESS_BUF_CAP }>,
}

trait Ingress<'d>: wire::Cursor<'d> {
    fn preserve(&mut self) -> Result<(), PreserveError>;
}

impl<'a, 'work, 'connection, 'd, const ID: u8, N: session::Session<'d, ID>>
    Parser<'a, 'work, 'connection, 'd, ID, N>
{
    pub(super) fn new(
        owner: &'a mut N,
        ingress: &'a mut resident::Snapshot<'d, { session::INGRESS_BUF_CAP }>,
        budget: &'a resident::Budget<'d>,
        parse_state: &'a mut <N::Codec as codec::Codec>::ParseState,
        context: &'a mut session::Ctx<'connection, 'd, N, ID>,
        work: schedule::Application<'work, 'd>,
    ) -> Self {
        Self {
            owner,
            ingress,
            budget,
            parse_state,
            context,
            work,
        }
    }

    pub(super) fn run(self) -> Outcome {
        let Self {
            owner,
            ingress,
            budget,
            parse_state,
            context,
            work,
        } = self;
        let mut cursor = wire::ResidentCursor::new(ingress);
        Core {
            owner,
            budget,
            parse_state,
            context,
            work,
        }
        .run(&mut cursor)
    }

    pub(super) fn run_admitted(self, permit: schedule::ApplicationPermit<'work, 'd>) -> Outcome {
        let Self {
            owner,
            ingress,
            budget,
            parse_state,
            context,
            work,
        } = self;
        let mut cursor = wire::ResidentCursor::new(ingress);
        Core {
            owner,
            budget,
            parse_state,
            context,
            work,
        }
        .run_admitted(&mut cursor, permit)
    }

    pub(super) fn run_retained<R: wire::Cursor<'d>>(self, cursor: &mut R) -> Outcome {
        let Self {
            owner,
            ingress,
            budget,
            parse_state,
            context,
            work,
        } = self;
        let mut core = Core {
            owner,
            budget,
            parse_state,
            context,
            work,
        };
        if ingress.is_empty() {
            return core.run(&mut Direct { cursor, ingress });
        }
        let preserved = {
            let mut direct = Direct {
                cursor: &mut *cursor,
                ingress: &mut *ingress,
            };
            direct.preserve()
        };
        if let Err(error) = preserved {
            return match error {
                PreserveError::Capacity => Outcome::Capacity,
                PreserveError::Contract => Outcome::Overrun,
            };
        }
        if ingress.is_empty() {
            return Outcome::Complete;
        }
        core.run(&mut wire::ResidentCursor::new(ingress))
    }
}

impl<'a, 'work, 'connection, 'd, const ID: u8, N: session::Session<'d, ID>>
    Core<'a, 'work, 'connection, 'd, ID, N>
{
    fn ready(&mut self) -> Option<Outcome> {
        if !self.owner.settle_responses(self.work, self.context) {
            return Some(Outcome::Yield);
        }
        if !<N::ConnState as lifecycle::Lifecycle>::wants_close(self.context.state).is_keep() {
            return Some(Outcome::Complete);
        }
        None
    }

    fn after_delivery(&mut self) -> Option<Outcome> {
        if !<N::ConnState as lifecycle::Lifecycle>::wants_close(self.context.state).is_keep() {
            return Some(Outcome::Complete);
        }
        self.ready()
    }

    fn ready_admitted(
        &mut self,
        permit: schedule::ApplicationPermit<'work, 'd>,
    ) -> AdmittedReady<'work, 'd> {
        let settlement = self
            .owner
            .settle_responses_admitted(permit, self.work, self.context);
        let ready = match settlement {
            session::AdmittedSettlement::Available(permit) => AdmittedReady::Available(permit),
            session::AdmittedSettlement::Consumed => AdmittedReady::Spent,
            session::AdmittedSettlement::Yield => {
                return AdmittedReady::Outcome(Outcome::Yield);
            }
        };
        if !<N::ConnState as lifecycle::Lifecycle>::wants_close(self.context.state).is_keep() {
            return AdmittedReady::Outcome(Outcome::Complete);
        }
        ready
    }

    fn preserve<S: Ingress<'d>>(source: &mut S, outcome: Outcome) -> Outcome {
        match source.preserve() {
            Ok(()) => outcome,
            Err(PreserveError::Capacity) => Outcome::Capacity,
            Err(PreserveError::Contract) => Outcome::Overrun,
        }
    }

    fn parse<S: Ingress<'d>>(&mut self, source: &S) -> Step {
        use codec::Parse;

        let parsed = <N::Codec as codec::Codec>::parse(
            self.owner.codec(),
            self.parse_state,
            codec::Input::new(source, self.budget),
        );
        let (head, consumed) = match parsed {
            Ok(Parse::NeedMore) => return Step::NeedMore,
            Ok(Parse::CapacityExhausted) => return Step::Capacity,
            Ok(Parse::Item { head, consumed }) => (head, consumed.get()),
            Err(error) => {
                self.owner.protocol_error(error, self.context);
                return Step::Close;
            }
        };
        if consumed > source.chunk().len() {
            self.context.close_with(lifecycle::CloseReason::Protocol);
            return Step::Close;
        }
        self.owner.response(head, self.context);
        Step::Item(consumed)
    }

    fn run<S: Ingress<'d>>(&mut self, source: &mut S) -> Outcome {
        if let Some(outcome) = self.ready() {
            return match outcome {
                Outcome::Yield => Self::preserve(source, Outcome::Yield),
                outcome => outcome,
            };
        }
        self.run_source(source)
    }

    fn run_admitted<S: Ingress<'d>>(
        &mut self,
        source: &mut S,
        permit: schedule::ApplicationPermit<'work, 'd>,
    ) -> Outcome {
        match self.ready_admitted(permit) {
            AdmittedReady::Outcome(Outcome::Yield) => {
                return Self::preserve(source, Outcome::Yield);
            }
            AdmittedReady::Outcome(outcome) => return outcome,
            AdmittedReady::Spent => return self.run_source(source),
            AdmittedReady::Available(permit) => drop(permit),
        }
        if source.is_empty() {
            return Outcome::Complete;
        }
        let consumed = match self.parse(source) {
            Step::NeedMore => return Self::preserve(source, Outcome::Complete),
            Step::Capacity => return Outcome::Capacity,
            Step::Close => return Outcome::Close,
            Step::Item(consumed) => consumed,
        };
        if source.consume(consumed) != consumed {
            self.context.close_with(lifecycle::CloseReason::Protocol);
            return Outcome::Close;
        }
        if let Some(outcome) = self.after_delivery() {
            return match outcome {
                Outcome::Yield => Self::preserve(source, Outcome::Yield),
                outcome => outcome,
            };
        }
        self.run_source(source)
    }

    fn run_source<S: Ingress<'d>>(&mut self, source: &mut S) -> Outcome {
        loop {
            if source.is_empty() {
                return Outcome::Complete;
            }
            if !self.work.take() {
                return Self::preserve(source, Outcome::Yield);
            }
            let consumed = match self.parse(source) {
                Step::NeedMore => return Self::preserve(source, Outcome::Complete),
                Step::Capacity => return Outcome::Capacity,
                Step::Close => return Outcome::Close,
                Step::Item(consumed) => consumed,
            };
            if source.consume(consumed) != consumed {
                self.context.close_with(lifecycle::CloseReason::Protocol);
                return Outcome::Close;
            }
            if let Some(outcome) = self.after_delivery() {
                return match outcome {
                    Outcome::Yield => Self::preserve(source, Outcome::Yield),
                    outcome => outcome,
                };
            }
        }
    }
}

impl<'d, R: wire::Cursor<'d>> wire::Cursor<'d> for Direct<'_, 'd, R> {
    fn chunk(&self) -> &[u8] {
        self.cursor.chunk()
    }

    fn consume(&mut self, requested: usize) -> usize {
        self.cursor.consume(requested)
    }

    fn remaining(&self) -> usize {
        self.cursor.remaining()
    }

    fn retain(
        &self,
        range: ops::Range<usize>,
        budget: &resident::Budget<'d>,
    ) -> Result<wire::RetainedBytes<'d>, wire::RetainError> {
        self.cursor.retain(range, budget)
    }
}

impl<'d, R: wire::Cursor<'d>> Ingress<'d> for Direct<'_, 'd, R> {
    fn preserve(&mut self) -> Result<(), PreserveError> {
        while !self.cursor.is_empty() {
            let bytes = self.cursor.chunk();
            if bytes.is_empty() {
                return Err(PreserveError::Contract);
            }
            let len = bytes.len();
            self.ingress
                .try_extend(bytes)
                .map_err(|_| PreserveError::Capacity)?;
            if self.cursor.consume(len) != len {
                return Err(PreserveError::Contract);
            }
        }
        Ok(())
    }
}

impl<'d, const MAX_CAPACITY: usize> Ingress<'d> for wire::ResidentCursor<'_, 'd, MAX_CAPACITY> {
    fn preserve(&mut self) -> Result<(), PreserveError> {
        Ok(())
    }
}

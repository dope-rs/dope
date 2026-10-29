use std::ops::ControlFlow;

use dope_core::driver::{route::Identity, schedule::app};
use dope_net::wire;
use flow::Policy as _;
use o3::buffer::resident;
use response::Outcome as _;

use crate::{
    app::receive::Failure,
    client::{
        conn,
        session::{
            self,
            app::parser::outcomes::Run,
            codec::{self, mode},
            response::{self, flow, work},
            turns,
        },
    },
};

pub(super) struct Core<
    'a,
    'connection,
    'e,
    'w,
    'd: 'w,
    ID: Identity,
    N: session::Parser<'d, ID>,
    W: wire::Spec + 'd,
> {
    pub(super) owner: &'a mut N,
    pub(super) budget: &'a resident::Budget<'d>,
    ingress: &'a mut session::Ingress<'w, 'd, ID, N, W>,
    pub(super) context: &'a mut turns::Context<'connection, 'e, 'd, N::ConnState, N::Send, ID>,
}

impl<'a, 'connection, 'e, 'w, 'd: 'w, ID: Identity, N: session::Parser<'d, ID>, W: wire::Spec + 'd>
    Core<'a, 'connection, 'e, 'w, 'd, ID, N, W>
{
    pub(super) const fn new(
        owner: &'a mut N,
        budget: &'a resident::Budget<'d>,
        ingress: &'a mut session::Ingress<'w, 'd, ID, N, W>,
        context: &'a mut turns::Context<'connection, 'e, 'd, N::ConnState, N::Send, ID>,
    ) -> Self {
        Self {
            owner,
            budget,
            ingress,
            context,
        }
    }

    pub(super) fn is_closing(&self) -> bool {
        !matches!(self.context.close_status(), conn::CloseStatus::Open)
    }

    pub(super) fn discard(&mut self) {
        self.ingress.discard(self.owner.codec());
    }

    pub(super) fn finish<'turn>(
        &mut self,
        state: &mut <N::Response as response::Plan>::Flow,
        mut work: app::Work<'turn, 'd>,
    ) -> Run {
        let Self {
            owner,
            budget,
            ingress,
            context,
        } = self;
        ingress.with(|input| {
            let (mut input, ended) = match work::Admission::admit_with(&mut work, input, |input| {
                session::Ingress::<'w, 'd, ID, N, W>::finish(input, owner, budget, context)
            }) {
                work::Capacity::Available(ended) => ended,
                work::Capacity::Exhausted(input) => {
                    state.record(true, flow::Record::Ready);
                    return (input, Run::Yield);
                }
            };
            let (delivery, end) = match ended {
                Ok(codec::Finish::Empty) => {
                    session::Ingress::<'w, 'd, ID, N, W>::clear(&mut input, owner.codec());
                    state.record(true, flow::Record::Done);
                    return (input, Run::Complete);
                }
                Ok(codec::Finish::CapacityExhausted) => {
                    state.record(true, flow::Record::Done);
                    return (input, Run::Failed(Failure::FinishCapacity));
                }
                Ok(codec::Finish::Item(delivery)) => (delivery, flow::End::Continue),
                Err(delivery) => (delivery, flow::End::Close),
            };
            if matches!(delivery.disposition(), response::Direct::RetryInput) {
                state.record(true, flow::Record::Blocked);
                return (input, Run::Blocked);
            }
            session::Ingress::<'w, 'd, ID, N, W>::clear(&mut input, owner.codec());
            let next = match Self::apply(owner, context, state, true, delivery, &mut work, end) {
                ControlFlow::Continue(()) => Run::Complete,
                ControlFlow::Break(next) => next,
            };
            (input, next)
        })
    }

    pub(super) fn run_ready<A: work::Admission>(
        &mut self,
        state: &mut <N::Response as response::Plan>::Flow,
        eof: bool,
        work: &mut A,
    ) -> ControlFlow<Run> {
        if self.is_closing() {
            state.record(eof, flow::Record::Done);
            return ControlFlow::Break(Run::Complete);
        }
        match self.ingress.prepare(self.budget) {
            Ok(true) => {}
            Ok(false) => {
                state.record(eof, flow::Record::Done);
                return ControlFlow::Break(Run::Complete);
            }
            Err(error) => {
                state.record(eof, flow::Record::Done);
                return ControlFlow::Break(Run::Failed(error));
            }
        }
        let step = match work.admit(|| self.ingress.decode(self.owner, self.budget, self.context)) {
            work::Capacity::Available(step) => step,
            work::Capacity::Exhausted(()) => {
                state.record(eof, flow::Record::Ready);
                return ControlFlow::Break(Run::Yield);
            }
        };
        let step = match step {
            mode::Read::Step(step) => step.0,
            mode::Read::Failed(error) => {
                state.record(eof, flow::Record::Done);
                return ControlFlow::Break(Run::Failed(error));
            }
            mode::Read::Close => {
                state.record(eof, flow::Record::Done);
                return ControlFlow::Break(Run::Close);
            }
        };
        match step {
            codec::Progress::More { advanced, target } => {
                state.record(eof, flow::Record::Done);
                match self.ingress.preserve(target, self.budget) {
                    Ok(more) if advanced || more => ControlFlow::Continue(()),
                    Ok(_) => ControlFlow::Break(Run::Complete),
                    Err(error) => ControlFlow::Break(Run::Failed(error)),
                }
            }
            codec::Progress::CapacityExhausted => {
                state.record(eof, flow::Record::Done);
                ControlFlow::Break(Run::Failed(Failure::ParseCapacity))
            }
            codec::Progress::Blocked | codec::Progress::RetryInput => {
                state.record(eof, flow::Record::Blocked);
                ControlFlow::Break(Run::Blocked)
            }
            codec::Progress::Rejected(delivery) => Self::apply(
                self.owner,
                self.context,
                state,
                eof,
                delivery,
                work,
                flow::End::Close,
            ),
            codec::Progress::Item(delivery) => Self::apply(
                self.owner,
                self.context,
                state,
                eof,
                delivery,
                work,
                flow::End::Continue,
            ),
        }
    }

    fn apply<A: work::Admission>(
        owner: &mut N,
        context: &mut turns::Context<'connection, 'e, 'd, N::ConnState, N::Send, ID>,
        state: &mut <N::Response as response::Plan>::Flow,
        eof: bool,
        delivery: <N::Response as response::Plan>::Delivery,
        work: &mut A,
        end: flow::End,
    ) -> ControlFlow<Run> {
        match <N::Response as response::Mode<'d, ID, N>>::apply(
            state, owner, delivery, work, context, eof, end,
        ) {
            response::Effect::Committed => {
                state.record(eof, flow::Record::Done);
                ControlFlow::Continue(())
            }
            response::Effect::Close => {
                state.record(eof, flow::Record::Done);
                ControlFlow::Break(Run::Close)
            }
            response::Effect::RetryInput => {
                state.record(eof, flow::Record::Blocked);
                ControlFlow::Break(Run::Blocked)
            }
            response::Effect::Yield => ControlFlow::Break(Run::Yield),
            response::Effect::Blocked => ControlFlow::Break(Run::Blocked),
        }
    }
}

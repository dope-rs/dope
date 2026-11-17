use std::{cell::Cell, convert::Infallible, ops::ControlFlow, pin::Pin, rc::Rc};

use dope_core::{
    Errno,
    driver::{route, route::kind, schedule},
    io::event::{Kind, batch::Flow, net::recv},
};
use dope_manifold::{
    app::dispatch::{self, event},
    client::close,
};
use dope_runtime::executor;
use dope_test::harness::{
    observe::Trace,
    signals::{Gate, Progress as _},
};

use crate::common::manifold::client::{
    connect::batch,
    finish::close::members::DemandState,
};

#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    Input,
    Eof,
    Failed(Errno),
    Closed(close::Reason),
}

pub struct State {
    pub(crate) error: Errno,
    pub(crate) queued: Gate,
    pub(crate) cancelled_flow: Cell<Option<Flow>>,
    pub(crate) events: Trace<Event, 4>,
}

#[pin_project::pin_project]
pub struct Host<'d, M> {
    #[pin]
    pub(crate) inner: M,
    pub(crate) state: Option<Rc<State>>,
    pub(crate) renewal: Option<Rc<DemandState>>,
    pub(crate) batch: Option<batch::Capture<'d>>,
    /// Identifies the injected completion retained by the executor for replay.
    pub(crate) pending: Option<route::Erased<'d, kind::Recv>>,
}

impl<'d, M: dispatch::raw::Manifold<'d>> dispatch::raw::Projection<'d> for Host<'d, M> {
    type Inner = M;

    fn inner(self: Pin<&Self>) -> Pin<&M> {
        self.project_ref().inner
    }

    fn inner_mut(self: Pin<&mut Self>) -> Pin<&mut M> {
        self.project().inner
    }
}

// SAFETY: all owner operations project the same pinned manifold; completion
// injection keeps the actual native target, flow, and owning dispatch call.
unsafe impl<'d, M> dispatch::raw::Manifold<'d> for Host<'d, M>
where
    M: dispatch::raw::Manifold<'d, PrePark = dispatch::raw::Retained>,
    M::Dispatch: event::Set<'d, M, M::Route, Plan = ()>,
{
    type Route = M::Route;
    type Dispatch = event::Timed<dispatch::raw::Retained>;
    type Install = dispatch::raw::Projected<Self>;
    type Finish = dispatch::raw::Projected<Self>;
    type Activate = dispatch::raw::Projected<Self>;
    type Probe = dispatch::raw::Projected<Self>;
    type PrePark = M::PrePark;
    type Shutdown = M::Shutdown;

    const COORDS: usize = M::COORDS;

    unsafe fn pre_park<'a, 'turn>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'd, Self::PrePark>,
    ) -> Pin<&'a mut Self> {
        // SAFETY: projection retains the exact installed, pinned owner.
        unsafe { M::pre_park(self.as_mut().project().inner, turn, driver) };
        self
    }

    fn shutdown<'a, 'turn>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'd, Self::Shutdown>,
    ) -> Pin<&'a mut Self> {
        M::shutdown(self.as_mut().project().inner, turn, driver);
        self
    }
}

impl<'d, M> event::Turn<'d> for Host<'d, M>
where
    M: dispatch::raw::Manifold<'d, PrePark = dispatch::raw::Retained>,
    M::Dispatch: event::Set<'d, M, M::Route, Plan = ()>,
{
    type Access = dispatch::raw::Retained;

    fn dispatch<'event>(
        owner: Pin<&mut Self>,
        done: executor::raw::Dispatch<'event, 'd>,
        turn: schedule::Turn<'_, 'd>,
        mut driver: dispatch::raw::Context<'_, 'd, Self::Access>,
    ) -> ControlFlow<executor::raw::Dispatch<'event, 'd>> {
        let mut this = owner.project();
        let (done, vacant) = done.into_event().take();
        let mut selected = false;
        let (kind, last) = match (done.into_kind(), this.batch.as_mut()) {
            (Kind::Recv(done), Some(capture)) => {
                let (done, last) = capture.receive(done);
                let Some(done) = done else { return ControlFlow::Continue(()) };
                (Kind::Recv(done), last)
            }
            (kind, _) => (kind, false),
        };
        if matches!(&kind, Kind::Connect(_))
            && let Some(demand) = this.renewal.take()
        {
            let pending = demand.pending.replace(None);
            let live = demand.live.replace(None);
            demand.needed.set(false);
            demand.withdrawn.set(false);
            assert_ne!(turn.maint().remaining(), 0, "native admission maintenance");
            turn.reborrow().maint().share::<{ schedule::LIMIT }, _>(|| {
                // SAFETY: the same pinned owner retains the undispatched native completion.
                unsafe { M::pre_park(this.inner.as_mut(), turn.reborrow(), &mut driver) };
            });
            assert!(
                demand.withdrawn.get(),
                "withdrawal is reconciled before native admission"
            );
            demand.needed.set(true);
            demand.pending.set(pending);
            demand.live.set(live);
        }
        let kind = match (kind, this.state.as_ref()) {
            (Kind::Recv(done), Some(state)) if state.queued.hits() == 0 => {
                let done = done.dispatch::<Infallible>(|token, flow, outcome| {
                    ControlFlow::Break(match outcome {
                        recv::Outcome::Cancelled if this.pending.is_none() => {
                            state.cancelled_flow.set(Some(flow));
                            *this.pending = Some(token);
                            selected = true;
                            recv::Outcome::Failed(state.error)
                        }
                        recv::Outcome::Failed(error)
                            if flow == Flow::Last
                                && error == state.error
                                && this.pending.is_some_and(|pending| pending.matches(token)) =>
                        {
                            selected = true;
                            recv::Outcome::Failed(error)
                        }
                        outcome => outcome,
                    })
                });
                Kind::Recv(match done {
                    ControlFlow::Break(done) => done,
                    ControlFlow::Continue(never) => match never {},
                })
            }
            (kind, _) => kind,
        };
        let result = <M::Dispatch as event::Set<'d, M, M::Route>>::call(
            this.inner,
            (),
            executor::raw::Dispatch::new(vacant.insert(kind.into())),
            turn,
            (&mut *driver).reborrow(),
        );
        if selected
            && result.is_continue()
            && let Some(state) = this.state.as_ref()
        {
            this.pending.take();
            state.queued.hit();
        }
        if last && result.is_continue() {
            this.batch.as_ref().expect("terminal batch").done.hit();
        }
        result
    }
}

use std::{cell::Cell, convert::Infallible, ops::ControlFlow, pin::Pin, rc::Rc};

use dope_core::{
    Errno,
    driver::{route, route::kind, schedule},
    io,
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

use crate::common::manifold::client::finish::close::members::DemandState;

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
    M: dispatch::raw::Manifold<'d, PrePark = dispatch::raw::Retained>
        + dispatch::raw::Projection<'d>,
    M::Inner: event::Turn<'d, Access = dispatch::raw::Retained>,
{
    type Route = M::Route;
    type Dispatch = event::Timed<<M::Inner as event::Turn<'d>>::Access>;
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
    M: dispatch::raw::Manifold<'d, PrePark = dispatch::raw::Retained>
        + dispatch::raw::Projection<'d>,
    M::Inner: event::Turn<'d, Access = dispatch::raw::Retained>,
{
    type Access = <M::Inner as event::Turn<'d>>::Access;

    fn dispatch<'app, 'driver, 'turn>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                schedule::Turn<'turn, 'd>,
                dispatch::raw::Context<'driver, 'd, Self::Access>,
            ),
        >,
    ) -> ControlFlow<io::Event<'d>> {
        let (owner, (), (done, turn, mut driver)) = call.into_parts();
        let mut this = owner.project();
        let mut selected = false;
        let kind = done.into_kind();
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
        let result = event::Turn::dispatch(executor::raw::Call::new(
            dispatch::raw::Projection::inner_mut(this.inner),
            (),
            (kind.into(), turn, driver),
        ));
        if selected
            && result.is_continue()
            && let Some(state) = this.state.as_ref()
        {
            this.pending.take();
            state.queued.hit();
        }
        result
    }
}

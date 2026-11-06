use std::{cell::Cell, ops::ControlFlow, pin::Pin};

use dope_core::{
    driver::{
        retained::flight,
        route::{KeyTag, kind},
        schedule,
    },
    io::{self, event::Kind, event::net::send, fd::handles},
};
use dope_manifold::app::dispatch::{self, event};
use dope_runtime::executor;

#[pin_project::pin_project]
pub struct Observed<'a, 'd, M> {
    #[pin]
    pub(crate) endpoint: M,
    pub(crate) socket: handles::Datagram<'d>,
    pub(crate) flights: flight::Slots<'d, KeyTag<0>>,
    pub(crate) shutdown: Option<flight::Flight<'d, kind::Send>>,
    pub(crate) completed: Option<send::Completion<'d>>,
    pub(crate) rejected: &'a Cell<Option<bool>>,
}

impl<'d, M: dispatch::raw::Manifold<'d>> dispatch::raw::Projection<'d> for Observed<'_, 'd, M> {
    type Inner = M;

    fn inner(self: Pin<&Self>) -> Pin<&M> {
        self.project_ref().endpoint
    }

    fn inner_mut(self: Pin<&mut Self>) -> Pin<&mut M> {
        self.project().endpoint
    }
}

// SAFETY: lifecycle operations retain the same installed, pinned endpoint.
// Only an owned terminal send completion is held; no receive deferral or
// borrowed dispatch context escapes. Both native operations finish before
// either completion is presented to the endpoint.
unsafe impl<'d, M> dispatch::raw::Manifold<'d> for Observed<'_, 'd, M>
where
    M: dispatch::raw::Manifold<'d, Route = KeyTag<0>, PrePark = dispatch::raw::Retained>
        + event::Drive<'d, Access = dispatch::raw::Plain>,
{
    type Route = KeyTag<0>;
    type Dispatch = event::Driven<dispatch::raw::Retained>;
    type Install = dispatch::raw::Projected<Self>;
    type Finish = dispatch::raw::Projected<Self>;
    type Activate = dispatch::raw::Projected<Self>;
    type Probe = dispatch::raw::Projected<Self>;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = M::Shutdown;

    const COORDS: usize = M::COORDS;

    unsafe fn pre_park<'a, 'turn>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'd, Self::PrePark>,
    ) -> Pin<&'a mut Self> {
        // SAFETY: projection preserves the exact installed endpoint and its storage.
        unsafe { M::pre_park(self.as_mut().project().endpoint, turn, driver) };
        self
    }

    fn shutdown<'a, 'turn>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'd, Self::Shutdown>,
    ) -> Pin<&'a mut Self> {
        M::shutdown(self.as_mut().project().endpoint, turn, driver);
        self
    }
}

impl<'d, M: event::Drive<'d, Access = dispatch::raw::Plain>> event::Drive<'d>
    for Observed<'_, 'd, M>
{
    type Access = dispatch::raw::Retained;

    fn dispatch<'app, 'driver>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                dispatch::raw::Context<'driver, 'd, Self::Access>,
            ),
        >,
    ) -> ControlFlow<io::Event<'d>> {
        let (owner, (), (done, mut driver)) = call.into_parts();
        let mut this = owner.project();
        let done = match done.into_kind() {
            Kind::Send(done) if this.rejected.get().is_none() => done,
            other => {
                return M::dispatch(executor::raw::Call::new(
                    this.endpoint,
                    (),
                    (other.into(), driver.narrow()),
                ));
            }
        };
        if let Some(completed) = this.completed.take() {
            let shutdown = this.shutdown.take().expect("submitted shutdown");
            assert!(
                shutdown.matches(done.token()),
                "terminal shutdown completion"
            );
            let result = M::dispatch(executor::raw::Call::new(
                this.endpoint.as_mut(),
                (),
                (Kind::Send(done).into(), driver.narrow()),
            ));
            this.rejected.set(Some(result.is_break()));
            drop(result);
            drop(shutdown);
            return M::dispatch(executor::raw::Call::new(
                this.endpoint,
                (),
                (Kind::Send(completed).into(), driver.narrow()),
            ));
        }
        let space = driver.driver_ref().targets::<KeyTag<0>>();
        let target = space.parse(done.token()).ok().expect("datagram send route");
        let target = space.bind(target.parts().slot(), target.parts().epoch());
        *this.shutdown = Some(
            this.flights
                .shutdown((*driver).reborrow(), this.socket, target)
                .expect("shutdown submission"),
        );
        *this.completed = Some(done);
        ControlFlow::Continue(())
    }
}

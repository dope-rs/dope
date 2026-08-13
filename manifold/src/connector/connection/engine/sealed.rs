use std::{ops, pin};

use dope_core::driver::{lifecycle, lifecycle::routing, schedule};
use dope_net::{
    link::pool::{self, pending},
    wire,
};
use o3::cell::region;

use crate::{
    connector::{
        app, attempt, auxiliary,
        connection::{
            self,
            engine::{
                scheduling::phase,
                transfer::{flush, send},
                transition::{connect, shutdown},
            },
        },
    },
    dispatch::typed,
    receive::ingress,
};

pub(super) struct PoolBinding<'d, const ID: u8, T, W, S, M, B, const IOV: usize>
where
    T: dope_net::Transport,
    W: wire::Wire,
{
    prepared: pool::raw::PreparedOutbound<'d, ID, T, W, S, M, B, IOV>,
}

impl<'d, const ID: u8, T, W, S, M, B, const IOV: usize> PoolBinding<'d, ID, T, W, S, M, B, IOV>
where
    T: dope_net::Transport,
    W: wire::Wire,
{
    pub(super) fn new(prepared: pool::raw::PreparedOutbound<'d, ID, T, W, S, M, B, IOV>) -> Self {
        Self { prepared }
    }

    pub(super) fn bind(
        self,
        route: routing::Route<'d, ID>,
    ) -> pool::Outbound<'d, ID, T, W, S, M, B, IOV> {
        // SAFETY: the only constructor consumer installs, shuts down, and
        // finishes the returned pool as part of its Manifold proof below.
        unsafe { self.prepared.bind(route) }
    }
}

// SAFETY: Engine drives every pinned connector slot through route quiescence.
unsafe impl<'d, const ID: u8, A, S, E, X> crate::dispatch::raw::Manifold<'d>
    for connection::Engine<'d, ID, A, S, E, X>
where
    A: app::Receive<'d, ID>
        + app::Lifecycle<'d, ID>
        + app::RequestSource<'d, ID>
        + app::Scheduling<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    const ID: u8 = ID;
    type Dispatch = crate::dispatch::raw::Retained;
    type Activate = crate::dispatch::raw::Retained;
    type PrePark = crate::dispatch::raw::Retained;
    type Shutdown = crate::dispatch::raw::Retained;

    fn install(self: pin::Pin<&mut Self>, install: &mut lifecycle::Install<'_, 'd>) {
        self.project().pool.install(install);
    }

    unsafe fn dispatch<'turn>(
        mut self: pin::Pin<&mut Self>,
        ev: crate::DriverEvent<'d>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Dispatch>,
    ) -> ops::ControlFlow<crate::DriverEvent<'d>> {
        use dope_core::io::event::Kind;
        match ev.into_kind() {
            Kind::Recv(completion) => {
                if let ops::ControlFlow::Break(completion) =
                    <Self as ingress::Policy<'d, ID>>::dispatch(
                        self.as_mut(),
                        completion,
                        turn.reborrow(),
                        driver,
                    )
                {
                    return ops::ControlFlow::Break(crate::DriverEvent::from(completion));
                }
                flush::FlushPhase::flush_dirty(self.as_mut(), turn.reborrow(), driver);
                return ops::ControlFlow::Continue(());
            }
            Kind::Send(completion) => {
                flush::FlushPhase::handle_send(self.as_mut(), completion, turn.reborrow(), driver);
            }
            Kind::Socket(completion) => {
                connect::ConnectPhase::socket(self.as_mut(), completion, turn.reborrow(), driver);
            }
            Kind::Connect(completion) => {
                connect::ConnectPhase::connect(self.as_mut(), completion, turn.reborrow(), driver);
            }
            _ => {}
        }
        self.as_mut()
            .project()
            .pool
            .ingress()
            .flush(turn.reborrow().maintenance(), driver);
        flush::FlushPhase::flush_dirty(self.as_mut(), turn.reborrow(), driver);
        ops::ControlFlow::Continue(())
    }

    unsafe fn pre_park<'turn>(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::PrePark>,
    ) {
        shutdown::ShutdownPhase::drain_shutdown(self.as_mut(), turn.reborrow(), driver);
        {
            let this = self.as_mut().project();
            let work = turn.reborrow().application();
            this.app.pre_park(work, driver.region_token());
        }
        self.as_mut().rouse(turn.reborrow(), driver);
        self.as_mut()
            .project()
            .pool
            .ingress()
            .flush(turn.maintenance(), driver);
    }

    unsafe fn activate<'turn>(
        mut self: pin::Pin<&mut Self>,
        target: typed::Token<'d, Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Activate>,
    ) {
        let target = target.raw();
        <Self as ingress::Policy<'d, ID>>::resume(self.as_mut(), target, turn.reborrow(), driver);
        send::SendPhase::apply_requests(self.as_mut(), target, turn.reborrow(), driver);
        self.as_mut().rouse(turn, driver);
    }

    fn progress(self: pin::Pin<&Self>, region: &region::Token<'d>) -> schedule::Progress<'d> {
        let this = self.project_ref();
        if !pending::Pending::of(this.pool).is_empty()
            || this.pool.inspection().pending_rearm()
            || this.auxiliary.has_requests()
            || this.auxiliary.has_cancellations()
        {
            return schedule::Progress::Runnable;
        }
        let io = if this.pool.inspection().has_io_targets() || this.pool.has_outbound_targets() {
            schedule::Progress::waiting(region)
        } else {
            schedule::Progress::Quiescent
        };
        this.app.progress(region).reduce(io)
    }

    fn shutdown_progress(
        self: pin::Pin<&Self>,
        region: &region::Token<'d>,
    ) -> schedule::Progress<'d> {
        let this = self.project_ref();
        if !pending::Pending::of(this.pool).is_empty()
            || this.pool.inspection().pending_rearm()
            || this.auxiliary.has_requests()
            || this.auxiliary.has_cancellations()
        {
            return schedule::Progress::Runnable;
        }
        match this.schedule.shutdown {
            phase::Shutdown::Closing(_) => this
                .app
                .progress(region)
                .reduce(schedule::Progress::waiting(region)),
            phase::Shutdown::Open | phase::Shutdown::Done => self.progress(region),
        }
    }

    fn shutdown<'turn>(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Shutdown>,
    ) {
        self.shutdown_all(turn, driver);
    }

    fn finish(self: pin::Pin<&mut Self>, finish: &mut lifecycle::Finalize<'_, 'd>) {
        let this = self.project();
        this.pool.finish(finish);
    }
}

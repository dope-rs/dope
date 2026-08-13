use std::{ops, pin};

use dope_core::driver::{
    lifecycle::{self, routing},
    schedule,
};
use dope_net::{
    link::pool::{self, pending},
    wire,
};
use o3::cell::region;

use crate::{
    dispatch::typed,
    listener::{
        self, accept, connection, handler,
        runtime::lifecycle::Lifecycle as _,
        writer::{self, phase},
    },
    receive::ingress,
};

// SAFETY: `Control` only exposes handler borrows and connection commands. It
// cannot move, replace, or drop the listener, its pool, or retained buffers.
unsafe impl<'d, const ID: u8, A, E> crate::dispatch::raw::Controlled<'d>
    for listener::Listener<'d, ID, A, E>
where
    A: handler::Application<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
{
    type Control<'step>
        = listener::Control<'step, 'd, ID, A, E>
    where
        Self: 'step,
        'd: 'step;

    unsafe fn control<'step>(self: pin::Pin<&'step mut Self>) -> Self::Control<'step>
    where
        'd: 'step,
    {
        listener::Control { inner: self }
    }
}

pub(in crate::listener) const IOV_CAP: usize = ingress::IOV_CAP;

pub(in crate::listener) trait Binding<'d, const ID: u8> {
    type Output;

    fn bind(self, route: routing::Route<'d, ID>) -> Self::Output;
}

impl<'d, const ID: u8, T, W, C, M> Binding<'d, ID>
    for pool::Prepared<
        'd,
        ID,
        T,
        W,
        connection::State<'d, ID, C>,
        M,
        writer::Payload<'d, ID>,
        { listener::IOV_CAP },
    >
where
    T: dope_net::Transport,
    W: wire::Wire,
{
    type Output = pool::Pool<
        'd,
        ID,
        T,
        W,
        connection::State<'d, ID, C>,
        M,
        writer::Payload<'d, ID>,
        { listener::IOV_CAP },
    >;

    fn bind(self, route: routing::Route<'d, ID>) -> Self::Output {
        unsafe { pool::raw::Bind::bind(self, route) }
    }
}

// SAFETY: Listener owns accept, ingress, send, and deadline phases through finish.
unsafe impl<'d, const ID: u8, A, E> crate::dispatch::raw::Manifold<'d>
    for listener::Listener<'d, ID, A, E>
where
    A: handler::Application<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
{
    const ID: u8 = ID;
    type Dispatch = crate::dispatch::raw::Retained;
    type Activate = crate::dispatch::raw::Retained;
    type PrePark = crate::dispatch::raw::Retained;
    type Shutdown = crate::dispatch::raw::Retained;

    fn install(self: pin::Pin<&mut Self>, install: &mut lifecycle::Install<'_, 'd>) {
        self.project().owner.pool().install(install);
    }

    unsafe fn dispatch<'turn>(
        self: pin::Pin<&mut Self>,
        ev: crate::DriverEvent<'d>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Dispatch>,
    ) -> ops::ControlFlow<crate::DriverEvent<'d>> {
        use dope_core::io::event::Kind;

        let mut this = self;
        match ev.into_kind() {
            Kind::Recv(completion) => {
                if let ops::ControlFlow::Break(completion) =
                    <Self as ingress::Policy<'d, ID>>::dispatch(
                        this.as_mut(),
                        completion,
                        turn.reborrow(),
                        driver,
                    )
                {
                    return ops::ControlFlow::Break(crate::DriverEvent::from(completion));
                }
                let fields = this.as_mut().project();
                let available = fields.owner.pool().inspection().available();
                accept::Source::arm(
                    fields.accept,
                    driver,
                    available,
                    turn.reborrow().maintenance(),
                );
                phase::Phase::flush_dirty(this, turn.reborrow(), driver);
                return ops::ControlFlow::Continue(());
            }
            Kind::Send(completion) => {
                use crate::listener::writer::send::SendPhase;

                SendPhase::pump_send(this.as_mut(), completion, turn.reborrow(), driver)
            }
            Kind::Accept(token, completion) => {
                accept::AcceptPhase::accept_inherent(
                    this.as_mut(),
                    token,
                    completion,
                    turn.reborrow(),
                    driver,
                );
            }
            Kind::Tuning(completion) => {
                accept::AcceptPhase::tuning_inherent(
                    this.as_mut(),
                    completion,
                    turn.reborrow(),
                    driver,
                );
            }
            _ => {}
        }
        let fields = this.as_mut().project();
        let available = fields.owner.pool().inspection().available();
        accept::Source::arm(
            fields.accept,
            driver,
            available,
            turn.reborrow().maintenance(),
        );
        fields
            .owner
            .pool_mut()
            .ingress()
            .flush(turn.reborrow().maintenance(), driver);
        phase::Phase::flush_dirty(this, turn.reborrow(), driver);
        ops::ControlFlow::Continue(())
    }

    unsafe fn activate<'turn>(
        mut self: pin::Pin<&mut Self>,
        target: typed::Token<'d, Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Activate>,
    ) {
        let target = target.raw();
        <Self as ingress::Policy<'d, ID>>::resume(self.as_mut(), target, turn.reborrow(), driver);
        let idx = {
            let fields = self.as_ref().project_ref();
            let Some((idx, _)) = fields.owner.pool().by_target(target) else {
                return;
            };
            idx
        };
        {
            let mut fields = self.as_mut().project();
            let Some(mut egress) = fields.owner.egress_mut(idx) else {
                return;
            };
            A::activate(
                fields.app.as_mut(),
                egress.context(turn.reborrow().application()),
                driver,
            );
        }
        phase::Phase::maybe_close_slot(self.as_mut(), idx, turn.reborrow(), driver);
        phase::Phase::flush_dirty(self, turn, driver);
    }

    fn shutdown<'turn>(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Shutdown>,
    ) {
        let mut this = self;
        {
            let fields = this.as_mut().project();
            fields.accept.stop_accept(driver);
            fields.schedule.begin_shutdown();
        }
        this.drain_shutdown(turn, driver);
    }

    fn finish(self: pin::Pin<&mut Self>, finish: &mut lifecycle::Finalize<'_, 'd>) {
        let this = self.project();
        this.accept.finish(finish);
        this.owner.pool_mut().finish(finish);
    }

    unsafe fn pre_park<'turn>(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::PrePark>,
    ) {
        use crate::listener::Phase;

        let mut this = self;
        this.as_mut().drain_shutdown(turn.reborrow(), driver);
        let now = driver.turn_now();
        for _ in 0..Phase::COUNT {
            let phase = this.as_mut().project().schedule.next_phase();
            match phase {
                Phase::Accept => {
                    let fields = this.as_mut().project();
                    let available = fields.owner.pool().inspection().available();
                    accept::Source::arm(
                        fields.accept,
                        driver,
                        available,
                        turn.reborrow().maintenance(),
                    );
                }
                Phase::Ingress => {
                    this.as_mut()
                        .project()
                        .owner
                        .pool_mut()
                        .ingress()
                        .flush(turn.reborrow().maintenance(), driver);
                }
                Phase::Dirty => phase::Phase::flush_dirty(this.as_mut(), turn.reborrow(), driver),
                Phase::Inbound => {
                    use crate::listener::Inbound;

                    this.as_mut()
                        .drain_deadline::<Inbound>(now, turn.reborrow(), driver);
                }
                Phase::Send => {
                    use crate::listener::SendDeadline;

                    this.as_mut()
                        .drain_deadline::<SendDeadline>(now, turn.reborrow(), driver);
                }
                Phase::Absolute => {
                    use crate::listener::Absolute;

                    this.as_mut()
                        .drain_deadline::<Absolute>(now, turn.reborrow(), driver);
                }
            }
            if turn.reborrow().maintenance().remaining() == 0 {
                return;
            }
        }
    }

    fn progress(self: pin::Pin<&Self>, region: &region::Token<'d>) -> schedule::Progress<'d> {
        let this = self.project_ref();
        if !pending::Pending::of(this.owner.pool()).is_empty()
            || this.owner.pool().inspection().pending_rearm()
        {
            return schedule::Progress::Runnable;
        }
        let deadline = [this.schedule.earliest(), A::deadline(this.app)]
            .into_iter()
            .flatten()
            .min();
        let pool = if this.owner.pool().inspection().has_io_targets() {
            schedule::Progress::waiting(region)
        } else {
            schedule::Progress::Quiescent
        };
        let io = this.accept.progress(region).reduce(pool);
        match deadline {
            Some(deadline) => io.reduce(schedule::Progress::until(region, deadline)),
            None => io,
        }
    }

    fn shutdown_progress(
        self: pin::Pin<&Self>,
        region: &region::Token<'d>,
    ) -> schedule::Progress<'d> {
        let this = self.project_ref();
        if !pending::Pending::of(this.owner.pool()).is_empty()
            || this.owner.pool().inspection().pending_rearm()
        {
            return schedule::Progress::Runnable;
        }
        if this.schedule.is_closing()
            || (this.schedule.is_done()
                && (this.accept.has_in_flight() || this.owner.pool().inspection().has_io_targets()))
        {
            schedule::Progress::waiting(region)
        } else {
            self.progress(region)
        }
    }
}

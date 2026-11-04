use std::{cell::Cell, ops::ControlFlow, pin::Pin, task};

use dope_core::{
    driver::{
        route::{self, KeyTag},
        schedule,
    },
    io,
};
use dope_manifold::{
    app::dispatch::{event, raw},
    client::{
        conn,
        side::{self, registry},
    },
};
use dope_runtime::executor;
use dope_test::harness::signals::LocalGateRef;
use o3::buffer::storage::Shared;

use super::Intent;
use crate::common::manifold::client::finish::close::observe::Events;

enum Phase {
    Waiting,
    Native,
    Running,
    Stopping,
}

#[pin_project::pin_project]
pub struct Retry<'a, 'd, M> {
    #[pin]
    inner: M,
    registry: &'a registry::Registry<'d, Intent<'d>>,
    events: &'a Cell<Events<'d>>,
    done: LocalGateRef<'a>,
    phase: Phase,
    pending: [Option<conn::Id<'d, KeyTag<0>>>; 2],
}

impl<'a, 'd, M> Retry<'a, 'd, M> {
    pub fn new(
        inner: M,
        registry: &'a registry::Registry<'d, Intent<'d>>,
        events: &'a Cell<Events<'d>>,
        done: LocalGateRef<'a>,
    ) -> Self {
        Self {
            inner,
            registry,
            events,
            done,
            phase: Phase::Waiting,
            pending: [None; 2],
        }
    }
}

impl<'d, M> raw::Projection<'d> for Retry<'_, 'd, M> {
    type Inner = M;

    fn inner(self: Pin<&Self>) -> Pin<&M> {
        self.project_ref().inner
    }

    fn inner_mut(self: Pin<&mut Self>) -> Pin<&mut M> {
        self.project().inner
    }
}

// SAFETY: all operations retain the same installed, pinned connector. The test
// advances its phases without polling the reactor or moving retained storage.
unsafe impl<'d, M> raw::Manifold<'d> for Retry<'_, 'd, M>
where
    M: raw::Manifold<'d, Route = KeyTag<0>, PrePark = raw::Retained, Shutdown = raw::Retained>
        + raw::Projection<'d>,
    M::Inner: event::Turn<'d, Access = raw::Retained>
        + raw::Activate<'d, M::Route, Access = raw::Retained>,
{
    type Route = M::Route;
    type Dispatch = event::Timed<raw::Retained>;
    type Install = raw::Projected<Self>;
    type Finish = raw::Projected<Self>;
    type Activate = raw::Retained;
    type Probe = raw::Projected<Self>;
    type PrePark = raw::Retained;
    type Shutdown = M::Shutdown;

    const COORDS: usize = M::COORDS;

    unsafe fn pre_park<'a, 'turn, 'root>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        let mut this = self.as_mut().project();
        // SAFETY: projection retains the exact installed owner and its loans.
        unsafe { M::pre_park(this.inner.as_mut(), turn.reborrow(), driver) };
        if matches!(this.phase, Phase::Waiting)
            && turn.maint().remaining() >= 16
            && turn.app().remaining() >= 4
        {
            let observed = this.events.take();
            let targets = match observed.conn.as_slice() {
                [first, second] => Some([*first, *second]),
                _ => None,
            };
            this.events.set(observed);
            if let Some(targets) = targets {
                let ready = this.registry.ready_target().expect("side registry ready");
                for target in targets {
                    assert!(
                        this.registry
                            .try_reserve(
                                Intent {
                                    target,
                                    payload: Shared::try_copy(b"old")
                                        .expect("initial side payload"),
                                },
                                ready.into()
                            )
                            .is_ok()
                    );
                }
                let before = turn.app().remaining();
                // SAFETY: both native creations remain in this installed owner.
                unsafe { M::pre_park(this.inner.as_mut(), turn.reborrow(), driver) };
                assert_eq!(
                    before - turn.app().remaining(),
                    2,
                    "both initial requests submitted"
                );
                for target in targets {
                    this.registry.abandon(target);
                }
                // SAFETY: cancellation returns authority but retains both native cells.
                unsafe { M::pre_park(this.inner.as_mut(), turn.reborrow(), driver) };
                for target in targets {
                    assert!(
                        this.registry
                            .try_reserve(
                                Intent {
                                    target,
                                    payload: Shared::try_copy(b"cancel")
                                        .expect("replacement side payload"),
                                },
                                ready.into()
                            )
                            .is_ok(),
                        "cancelled registry slot returned"
                    );
                }
                let before = turn.app().remaining();
                // SAFETY: no CQE has been dispatched, so both native slots remain busy.
                unsafe { M::pre_park(this.inner.as_mut(), turn.reborrow(), driver) };
                assert_eq!(
                    before - turn.app().remaining(),
                    2,
                    "both replacements attempted"
                );
                for target in targets {
                    assert!(this.registry.poll(target, ready.into()).is_pending());
                }
                *this.pending = targets.map(Some);
                *this.phase = Phase::Native;
            }
        }
        if matches!(this.phase, Phase::Running)
            && let Some(ready) = this.registry.ready_target()
        {
            for pending in &mut *this.pending {
                if let Some(target) = *pending
                    && let task::Poll::Ready(result) = this.registry.poll(target, ready.into())
                {
                    assert!(result.is_ok(), "native side request completes");
                    *pending = None;
                    this.done.hit();
                }
            }
            if this.pending.iter().all(Option::is_none) {
                *this.phase = Phase::Stopping;
                ready.wake();
            }
        }
        self
    }

    fn shutdown<'a, 'turn, 'root>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut raw::Context<'_, 'root, 'd, Self::Shutdown>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        M::shutdown(self.as_mut().project().inner, turn, driver);
        self
    }
}

impl<'d, M> event::Turn<'d> for Retry<'_, 'd, M>
where
    M: raw::Manifold<'d, Route = KeyTag<0>, PrePark = raw::Retained> + raw::Projection<'d>,
    M::Inner: event::Turn<'d, Access = raw::Retained>,
{
    type Access = raw::Retained;

    fn dispatch<'app, 'driver, 'turn, 'root>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                schedule::Turn<'turn, 'd>,
                raw::Context<'driver, 'root, 'd, Self::Access>,
            ),
        >,
    ) -> ControlFlow<io::Event<'d>>
    where
        'd: 'root,
    {
        let (owner, (), (done, turn, mut driver)) = call.into_parts();
        let mut this = owner.project();
        let kind = done.into_kind();
        let check =
            matches!(this.phase, Phase::Native) && matches!(&kind, io::event::Kind::Socket(_));
        let result = event::Turn::dispatch(executor::raw::Call::new(
            raw::Projection::inner_mut(this.inner.as_mut()),
            (),
            (kind.into(), turn.reborrow(), driver.reborrow()),
        ));
        if check {
            assert!(result.is_continue(), "native creation consumed");
            assert!(turn.maint().remaining() >= 2, "retry quota available");
            for limit in [0, 1] {
                turn.maint().share::<{ schedule::LIMIT }, _>(|| {
                    if limit == 0 {
                        while turn.maint().permit().is_some() {}
                    }
                    assert_eq!(turn.maint().remaining(), limit);
                    let before = turn.app().remaining();
                    // SAFETY: the same pinned owner retains every native loan;
                    // no native dispatch or unrestricted activation intervenes.
                    unsafe { M::pre_park(this.inner.as_mut(), turn.reborrow(), &mut driver) };
                    assert_eq!(
                        turn.app().remaining(),
                        before,
                        "partial retry promotion must not submit"
                    );
                    assert!(
                        matches!(
                            <M::Probe as raw::Probe<'d, M>>::progress(
                                this.inner.as_ref(),
                                driver.regions(),
                                turn.clock(),
                            ),
                            schedule::Progress::Runnable,
                        ),
                        "unpromoted due retries remain runnable"
                    );
                });
            }
            *this.phase = Phase::Running;
        }
        result
    }
}

// SAFETY: the actual activation retains its canonical target and pinned owner;
// shutdown and its queued requests remain in that owner through quiescence.
unsafe impl<'d, M> raw::Activate<'d, M::Route> for Retry<'_, 'd, M>
where
    M: raw::Manifold<'d, Route = KeyTag<0>, Shutdown = raw::Retained> + raw::Projection<'d>,
    M::Inner: raw::Activate<'d, M::Route, Access = raw::Retained>,
{
    type Access = raw::Retained;

    unsafe fn activate<'turn>(
        self: Pin<&mut Self>,
        target: route::Target<'d, M::Route>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut raw::Context<'_, '_, 'd, Self::Access>,
    ) {
        let mut this = self.project();
        if matches!(this.phase, Phase::Stopping)
            && let Some(ready) = this.registry.ready_target()
        {
            let observed = this.events.take();
            let [first, second] = observed.conn.as_slice() else {
                panic!("two primary targets");
            };
            let targets = [*first, *second];
            this.events.set(observed);
            for target in targets {
                assert!(
                    this.registry
                        .try_reserve(
                            Intent {
                                target,
                                payload: Shared::try_copy(b"closed")
                                    .expect("closed intake payload"),
                            },
                            ready.into()
                        )
                        .is_ok()
                );
            }
            turn.maint().share::<{ schedule::LIMIT }, _>(|| {
                while turn.maint().permit().is_some() {}
                M::shutdown(this.inner.as_mut(), turn.reborrow(), &mut driver.reborrow());
            });
            let before = turn.app().remaining();
            // SAFETY: this is the actual delivered target for the same pinned engine.
            unsafe {
                M::Inner::activate(
                    this.inner.as_mut().inner_mut(),
                    target,
                    turn.reborrow(),
                    driver,
                );
            }
            assert_eq!(
                turn.app().remaining(),
                before,
                "closed intake must not submit"
            );
            M::shutdown(this.inner.as_mut(), turn.reborrow(), &mut driver.reborrow());
            for target in targets {
                assert!(matches!(
                    this.registry.poll(target, ready.into()),
                    task::Poll::Ready(Err(side::Error::IntakeClosed))
                ));
                this.done.hit();
            }
            return;
        }
        // SAFETY: projection preserves the actual target and installed owner.
        unsafe { M::Inner::activate(this.inner.inner_mut(), target, turn, driver) };
    }
}

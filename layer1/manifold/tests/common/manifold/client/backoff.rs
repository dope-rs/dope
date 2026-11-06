use std::{cell::Cell, pin::Pin};

use dope_core::driver::{route, schedule, schedule::clock};
use dope_manifold::app::dispatch::raw;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Injection {
    Waiting,
    Queued {
        slot: route::indices::SlotIndex,
        control: bool,
        connection: bool,
    },
    Done,
}

#[pin_project::pin_project]
pub struct Exhausted<'a, 'd, M, F> {
    #[pin]
    pub(crate) inner: M,
    injected: &'a Cell<Injection>,
    due: clock::Deadline<'d>,
    wake: Option<F>,
}

impl<'a, 'd, M, F> Exhausted<'a, 'd, M, F>
where
    F: FnOnce(Pin<&mut M>) -> route::indices::SlotIndex,
{
    pub fn new(inner: M, due: clock::Deadline<'d>, injected: &'a Cell<Injection>, wake: F) -> Self {
        Self {
            inner,
            injected,
            due,
            wake: Some(wake),
        }
    }
}

fn exhaust(turn: schedule::Turn<'_, '_>, run: impl FnOnce()) {
    turn.maint().share::<{ schedule::LIMIT }, _>(|| {
        assert_eq!(turn.maint().remaining(), 1);
        let _permit = turn.maint().permit().expect("activation share");
        run();
    });
}

impl<'d, M, F> raw::Projection<'d> for Exhausted<'_, 'd, M, F> {
    type Inner = M;

    fn inner(self: Pin<&Self>) -> Pin<&M> {
        self.project_ref().inner
    }

    fn inner_mut(self: Pin<&mut Self>) -> Pin<&mut M> {
        self.project().inner
    }
}

// SAFETY: every operation retains the same installed, pinned connector and engine.
unsafe impl<'d, M, F> raw::Manifold<'d> for Exhausted<'_, 'd, M, F>
where
    M: raw::Manifold<'d, PrePark = raw::Retained> + raw::Projection<'d>,
    M::Inner: raw::Activate<'d, M::Route, Access = raw::Retained>,
    F: FnOnce(Pin<&mut M>) -> route::indices::SlotIndex,
{
    type Route = M::Route;
    type Dispatch = raw::Projected<Self>;
    type Install = raw::Projected<Self>;
    type Finish = raw::Projected<Self>;
    type Activate = raw::Retained;
    type Probe = raw::Projected<Self>;
    type PrePark = raw::Retained;
    type Shutdown = M::Shutdown;

    const COORDS: usize = M::COORDS;

    unsafe fn pre_park<'a, 'turn>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut raw::Context<'_, 'd, Self::PrePark>,
    ) -> Pin<&'a mut Self> {
        let mut this = self.as_mut().project();
        match this.injected.get() {
            Injection::Waiting if *this.due <= turn.clock().now() => {
                let slot = this.wake.take().expect("survivor wake")(this.inner.as_mut());
                this.injected.set(Injection::Queued {
                    slot,
                    control: false,
                    connection: false,
                });
                exhaust(turn.reborrow(), || {
                    // SAFETY: projection retains the installed connector and its loans.
                    unsafe { M::pre_park(this.inner.as_mut(), turn.reborrow(), driver) };
                });
            }
            Injection::Queued {
                control,
                connection,
                ..
            } => {
                assert!(control && connection, "both ready targets were consumed");
                let before = turn.maint().remaining();
                turn.maint().share::<{ schedule::LIMIT }, _>(|| {
                    assert_eq!(turn.maint().remaining(), 1);
                    // SAFETY: the exact installed owner receives one real maintenance permit.
                    unsafe { M::pre_park(this.inner.as_mut(), turn.reborrow(), driver) };
                    assert_eq!(turn.maint().remaining(), 0, "quiet dirty head consumed");
                });
                assert_eq!(turn.maint().remaining(), before - 1);
                assert!(
                    turn.maint().remaining() > 0,
                    "unused sibling share refunded"
                );
                this.injected.set(Injection::Done);
            }
            _ => {
                // SAFETY: projection retains the exact installed connector and its loans.
                unsafe { M::pre_park(this.inner.as_mut(), turn.reborrow(), driver) };
            }
        }
        self
    }

    fn shutdown<'a, 'turn>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut raw::Context<'_, 'd, Self::Shutdown>,
    ) -> Pin<&'a mut Self> {
        M::shutdown(self.as_mut().project().inner, turn, driver);
        self
    }
}

// SAFETY: activation retains the canonical route and the same pinned engine.
unsafe impl<'d, M, F> raw::Activate<'d, M::Route> for Exhausted<'_, 'd, M, F>
where
    M: raw::Manifold<'d, PrePark = raw::Retained> + raw::Projection<'d>,
    M::Inner: raw::Activate<'d, M::Route, Access = raw::Retained>,
    F: FnOnce(Pin<&mut M>) -> route::indices::SlotIndex,
{
    type Access = raw::Retained;

    unsafe fn activate<'turn>(
        self: Pin<&mut Self>,
        target: route::Target<'d, M::Route>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut raw::Context<'_, 'd, Self::Access>,
    ) {
        let mut this = self.project();
        let mut run = || {
            // SAFETY: projection preserves the canonical target and exact installed engine.
            unsafe {
                M::Inner::activate(
                    this.inner.as_mut().inner_mut(),
                    target,
                    turn.reborrow(),
                    driver,
                );
            }
        };
        if let Injection::Queued {
            slot,
            mut control,
            mut connection,
        } = this.injected.get()
        {
            if target.slot() == slot {
                connection = true;
            } else {
                assert_eq!(target.slot(), route::indices::SlotIndex::fixed::<2>());
                control = true;
            }
            exhaust(turn.reborrow(), run);
            this.injected.set(Injection::Queued {
                slot,
                control,
                connection,
            });
        } else {
            run();
        }
    }
}

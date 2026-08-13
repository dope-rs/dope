use std::{cell, marker, time};

use o3::{cell::region, collections::batch::set, mem::quota};

mod credits;
mod progress;
pub mod ready;
pub mod reservation;
pub mod timer;
mod wait;

pub(crate) use credits::Budget;
pub use progress::Progress;
pub use wait::Wait;

#[must_use = "admission reports whether work was acquired or the budget was exhausted"]
pub enum Admission<T> {
    Item(T),
    Empty,
    Exhausted,
}

use crate::driver::{self, route};

/// Hard ceiling for every scheduler work class in one driver turn.
pub const MAX_TURN_WORK_BUDGET: usize = 256;
pub(crate) const REACTOR_LANES: u8 = 2;

enum ReactorLane {}
enum CoordinationLane {}
enum ApplicationLane {}
enum MaintenanceLane {}

pub(crate) struct Work {
    reactor: quota::Ledger<ReactorLane>,
    reactor_cursor: cell::Cell<u8>,
    ready: quota::Ledger<ready::Lane>,
    coordination: quota::Ledger<CoordinationLane>,
    application: quota::Ledger<ApplicationLane>,
    timers: quota::Ledger<timer::Lane>,
    maintenance: quota::Ledger<MaintenanceLane>,
    maintenance_half: cell::Cell<Option<usize>>,
}

pub struct Controller<'scope, 'd> {
    driver: driver::Reference<'d>,
    work: &'scope mut Work,
}

#[must_use = "an active scheduler turn must remain live for the complete driver cycle"]
pub struct ActiveTurn<'turn, 'd> {
    driver: driver::Reference<'d>,
    work: &'turn mut Work,
}

const _: () = {
    assert!(
        std::mem::size_of::<Controller<'static, 'static>>() == 2 * std::mem::size_of::<usize>()
    );
    assert!(
        std::mem::size_of::<ActiveTurn<'static, 'static>>() == 2 * std::mem::size_of::<usize>()
    );
};

#[derive(Clone, Copy)]
#[repr(transparent)]
#[doc = include_str!("turn.md")]
pub struct Turn<'turn, 'd> {
    work: &'turn Work,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

#[doc = include_str!("share.md")]
#[must_use = "maintenance share must remain live while its participant runs"]
pub struct Share<'turn, 'd, const PARTICIPANTS: usize> {
    _reserve: credits::Quota<'turn, 'd>,
    _participants: marker::PhantomData<ParticipantCount<PARTICIPANTS>>,
}

struct ParticipantCount<const PARTICIPANTS: usize>;

const _: () =
    assert!(std::mem::size_of::<Share<'static, 'static, 1>>() == 2 * std::mem::size_of::<usize>());

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Half<'turn, 'd> {
    work: &'turn Work,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

#[doc(hidden)]
#[must_use = "reactor authority must be consumed by Poll or Source"]
pub struct Reactor<'turn, 'd> {
    quota: credits::Quota<'turn, 'd>,
    cursor: u8,
    _turn: marker::PhantomData<&'turn mut ()>,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Application<'turn, 'd> {
    remaining: &'turn quota::Ledger<ApplicationLane>,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

/// One non-duplicable application transition admitted by a driver turn.
///
/// The permit is created only by spending one unit from the turn ledger. Its
/// invariant driver brand lets a driver-owned consumer require the same
/// branded lifetime instead of relying on a runtime owner comparison.
///
/// A permit cannot be duplicated:
///
/// ```compile_fail,E0382
/// use dope_core::driver::schedule::ApplicationPermit;
///
/// fn duplicate<'turn, 'd>(permit: ApplicationPermit<'turn, 'd>) {
///     let moved = permit;
///     drop(permit);
///     drop(moved);
/// }
/// ```
#[must_use = "an application permit represents one already-spent transition"]
pub struct ApplicationPermit<'turn, 'd> {
    _turn: marker::PhantomData<&'turn mut ()>,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

#[must_use = "admission reports whether work was acquired or the application budget was exhausted"]
pub enum ApplicationAdmission<'turn, 'd, T> {
    Item(T, ApplicationPermit<'turn, 'd>),
    Empty,
    Exhausted(T),
}

const _: () = {
    use std::mem;

    assert!(mem::size_of::<ApplicationPermit<'static, 'static>>() == 0);
    assert!(
        mem::size_of::<ApplicationAdmission<'static, 'static, usize>>()
            == mem::size_of::<quota::Admission<usize>>()
    );
};

/// Linear coordination work class for one driver turn.
#[doc(hidden)]
pub struct Coordination<'turn, 'd> {
    remaining: &'turn quota::Ledger<CoordinationLane>,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
#[doc(hidden)]
pub struct Timers<'turn, 'd> {
    remaining: &'turn quota::Ledger<timer::Lane>,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Maintenance<'turn, 'd> {
    work: &'turn Work,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

#[doc = include_str!("maintenance_permit.md")]
#[must_use = "dropping the permit consumes its admitted maintenance transition"]
pub struct MaintenancePermit<'turn, 'd> {
    _turn: marker::PhantomData<&'turn mut ()>,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'turn, 'd> Turn<'turn, 'd> {
    pub fn reborrow(&self) -> Turn<'_, 'd> {
        Turn {
            work: self.work,
            _driver: marker::PhantomData,
        }
    }

    pub fn application(self) -> Application<'turn, 'd> {
        Application {
            remaining: &self.work.application,
            _driver: marker::PhantomData,
        }
    }

    #[doc(hidden)]
    pub fn coordination(self) -> Coordination<'turn, 'd> {
        Coordination {
            remaining: &self.work.coordination,
            _driver: marker::PhantomData,
        }
    }

    pub fn maintenance(self) -> Maintenance<'turn, 'd> {
        Maintenance {
            work: self.work,
            _driver: marker::PhantomData,
        }
    }

    #[doc(hidden)]
    pub fn timers(self) -> Timers<'turn, 'd> {
        Timers {
            remaining: &self.work.timers,
            _driver: marker::PhantomData,
        }
    }

    pub fn drain_ready(
        self,
        driver: driver::Reference<'d>,
        limit: usize,
        activate: impl FnMut(route::Token),
    ) -> usize {
        let mut budget = Budget::from_ready(self, limit);
        ready::Access::with(&driver, |access| {
            driver.ready().arena().drain(access, &mut budget, activate)
        })
    }
}

impl<'turn, 'd> Reactor<'turn, 'd> {
    pub(crate) fn remaining(&self) -> usize {
        self.quota.remaining()
    }

    pub(crate) const fn cursor(&self) -> usize {
        self.cursor as usize
    }

    pub(crate) fn budget<Lane>(&self, limit: usize) -> Budget<'_, 'd, Lane> {
        self.quota.reserve_budget(limit)
    }
}

impl Application<'_, '_> {
    pub fn remaining(self) -> usize {
        self.remaining.remaining()
    }

    /// Spends one credit for work completed directly in this call.
    /// Use [`Application::permit`] when the admitted transition is delegated
    /// to another API so the credit cannot be duplicated before it is used.
    pub fn take(self) -> bool {
        self.remaining.take()
    }

    pub fn admit_with<T>(self, acquire: impl FnOnce() -> Option<T>) -> Admission<T> {
        match self.remaining.admit_with(acquire) {
            quota::Admission::Item(value) => Admission::Item(value),
            quota::Admission::Empty => Admission::Empty,
            quota::Admission::Exhausted => Admission::Exhausted,
        }
    }
}

impl<'turn, 'd> Application<'turn, 'd> {
    /// Spends one application credit and returns its linear proof.
    pub fn permit(self) -> Option<ApplicationPermit<'turn, 'd>> {
        self.take().then_some(ApplicationPermit {
            _turn: marker::PhantomData,
            _driver: marker::PhantomData,
        })
    }

    pub fn admit_next<I: set::DenseIndex>(
        self,
        ready: &mut set::Drain<'_, I>,
    ) -> ApplicationAdmission<'turn, 'd, I> {
        match ready.next_with_quota(self.remaining) {
            set::Next::Item(value) => ApplicationAdmission::Item(
                value,
                ApplicationPermit {
                    _turn: marker::PhantomData,
                    _driver: marker::PhantomData,
                },
            ),
            set::Next::Empty => ApplicationAdmission::Empty,
            set::Next::Exhausted(value) => ApplicationAdmission::Exhausted(value),
        }
    }
}

impl Coordination<'_, '_> {
    #[doc(hidden)]
    pub fn take(&mut self) -> bool {
        self.remaining.take()
    }
}

impl<'turn, 'd> Timers<'turn, 'd> {
    pub fn remaining(self) -> usize {
        self.remaining.remaining()
    }

    pub fn take(self) -> bool {
        self.remaining.take()
    }
}

impl<'turn, 'd> Maintenance<'turn, 'd> {
    pub fn remaining(self) -> usize {
        self.work.maintenance.remaining()
    }

    pub fn take(self) -> bool {
        self.work.maintenance.take()
    }

    pub fn share<const PARTICIPANTS: usize>(self) -> Share<'turn, 'd, PARTICIPANTS> {
        const { assert!(PARTICIPANTS != 0) };
        if PARTICIPANTS == 1 {
            return Share {
                _reserve: credits::Quota::from_maintenance(self, 0),
                _participants: marker::PhantomData,
            };
        }
        let total = self.work.maintenance.remaining();
        let quota = total.div_ceil(PARTICIPANTS);
        let reserve = total - quota;
        Share {
            _reserve: credits::Quota::from_maintenance(self, reserve),
            _participants: marker::PhantomData,
        }
    }

    /// Returns the persistent half-turn quota for one maintenance producer.
    /// Re-entering in the same turn resumes the original quota rather than
    /// taking half of the remaining global budget again.
    pub fn half(self) -> Half<'turn, 'd> {
        if self.work.maintenance_half.get().is_none() {
            self.work
                .maintenance_half
                .set(Some(self.work.maintenance.remaining().div_ceil(2)));
        }
        Half {
            work: self.work,
            _driver: marker::PhantomData,
        }
    }
}

impl Half<'_, '_> {
    pub fn remaining(self) -> usize {
        match self.work.maintenance_half.get() {
            Some(quota) => self.work.maintenance.remaining().min(quota),
            None => self.work.maintenance.remaining(),
        }
    }

    pub fn take(self) -> bool {
        let Some(quota) = self.work.maintenance_half.get() else {
            return false;
        };
        if quota == 0 || self.work.maintenance.remaining() == 0 {
            return false;
        }
        self.work.maintenance_half.set(Some(quota - 1));
        let admitted = self.work.maintenance.take();
        debug_assert!(admitted);
        admitted
    }
}

impl<'turn, 'd> MaintenancePermit<'turn, 'd> {
    pub fn try_take(work: Maintenance<'turn, 'd>) -> Option<Self> {
        if !work.take() {
            return None;
        }
        Some(Self {
            _turn: marker::PhantomData,
            _driver: marker::PhantomData,
        })
    }

    /// Admits one transition and its matching region under one turn borrow.
    #[doc(hidden)]
    pub fn try_take_with_region<'context, 'access>(
        work: Maintenance<'turn, 'd>,
        context: &'access mut driver::Context<'context, 'd>,
    ) -> Option<(Self, &'access mut region::Token<'d>)> {
        if !work.take() {
            return None;
        }
        Some((
            Self {
                _turn: marker::PhantomData,
                _driver: marker::PhantomData,
            },
            context.region,
        ))
    }
}

impl Work {
    pub(crate) fn new() -> Self {
        Self {
            reactor: quota::Ledger::new(MAX_TURN_WORK_BUDGET),
            reactor_cursor: cell::Cell::new(0),
            ready: quota::Ledger::new(MAX_TURN_WORK_BUDGET),
            coordination: quota::Ledger::new(MAX_TURN_WORK_BUDGET),
            application: quota::Ledger::new(MAX_TURN_WORK_BUDGET),
            timers: quota::Ledger::new(MAX_TURN_WORK_BUDGET),
            maintenance: quota::Ledger::new(MAX_TURN_WORK_BUDGET),
            maintenance_half: cell::Cell::new(None),
        }
    }

    fn set(&mut self, value: usize) {
        self.reactor.reset(value);
        self.ready.reset(value);
        self.coordination.reset(value);
        self.application.reset(value);
        self.timers.reset(value);
        self.maintenance.reset(value);
        self.maintenance_half.set(None);
    }

    fn exhausted(&self) -> bool {
        self.reactor.remaining() == 0
            || self.ready.remaining() == 0
            || self.coordination.remaining() == 0
            || self.application.remaining() == 0
            || self.timers.remaining() == 0
            || self.maintenance.remaining() == 0
    }
}

impl<'scope, 'd> Controller<'scope, 'd> {
    pub(in crate::driver) fn new(driver: driver::Reference<'d>, work: &'scope mut Work) -> Self {
        Self { driver, work }
    }

    pub fn begin(&mut self, limit: usize) -> ActiveTurn<'_, 'd> {
        self.work.set(limit.min(MAX_TURN_WORK_BUDGET));
        ActiveTurn {
            driver: self.driver,
            work: &mut *self.work,
        }
    }
}

impl<'turn, 'd> ActiveTurn<'turn, 'd> {
    pub fn turn(&self) -> Turn<'_, 'd> {
        Turn {
            work: &*self.work,
            _driver: marker::PhantomData,
        }
    }

    #[doc(hidden)]
    pub fn refresh_clock(&self) -> time::Instant {
        let now = time::Instant::now();
        self.driver.scheduler().set_turn_now(now);
        now
    }

    #[doc(hidden)]
    pub fn reactor(&mut self) -> Reactor<'_, 'd> {
        self.reactor_with_turn().0
    }

    /// Splits the reactor's linear quota from a copyable view of the other
    /// turn lanes so source-owned completions can be dispatched immediately.
    #[doc(hidden)]
    pub fn reactor_with_turn(&mut self) -> (Reactor<'_, 'd>, Turn<'_, 'd>) {
        let work = &*self.work;
        let cursor = work.reactor_cursor.get();
        work.reactor_cursor.set((cursor + 1) % REACTOR_LANES);
        (
            Reactor {
                quota: credits::Quota::from_reactor(&work.reactor),
                cursor,
                _turn: marker::PhantomData,
                _driver: marker::PhantomData,
            },
            Turn {
                work,
                _driver: marker::PhantomData,
            },
        )
    }

    pub fn exhausted(&self) -> bool {
        self.work.exhausted()
    }

    pub fn drain_ready(&mut self, limit: usize, activate: impl FnMut(route::Token)) -> usize {
        self.turn().drain_ready(self.driver, limit, activate)
    }
}

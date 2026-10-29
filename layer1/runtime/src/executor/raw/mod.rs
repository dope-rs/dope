//! Raw application boundary. [`App`] owns complete lifecycle traversal;
//! runtime-owned session roots choose when it may run.
use std::{convert, mem, num::NonZero, pin};

use dope_core::driver::{
    self,
    life::{self, finish},
    retained, schedule,
};
use o3::base::{
    cell::region,
    num::bounded::{self, U32},
};

use crate::executor;

mod activate;
mod app;
mod cursor;
mod dispatch;
mod owner;
mod park;
mod progress;
mod released;

pub use activate::Activate;
pub use app::{App, Layout};
pub use cursor::Cursor;
pub use dispatch::Dispatch;
pub(crate) use owner::{Backing, Draining, Live, Owner, Parent, View};
pub use park::Park;
pub use progress::Progress;
pub use released::Released;

/// Cooperative drain while the complete application remains installed.
///
/// # Safety
/// Preserve every admitted owner's identity, presence, address, and routes through
/// finish. `begin` and `force` must not replace owners or reclaim retained storage.
/// Cooperative quiescence does not authorize reclamation: the runtime still
/// shuts down every admitted owner and drains native operations before finish.
pub unsafe trait Drain<'d>: executor::raw::App<'d> {
    fn begin(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn progress(
        self: pin::Pin<&Self>,
        region: &region::Token<region::Identity<'d>>,
    ) -> schedule::Progress<'d>;

    fn force(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

/// One exact capability invocation joining its pinned owner, selection, and operation.
pub struct Call<'app, R, P, O> {
    app: pin::Pin<&'app mut R>,
    plan: P,
    op: O,
}

impl<'app, R, P, O> Call<'app, R, P, O> {
    /// Joins one runtime-owned operation with its exact pinned root.
    pub const fn new(app: pin::Pin<&'app mut R>, plan: P, op: O) -> Self {
        Self { app, plan, op }
    }

    pub fn into_parts(self) -> (pin::Pin<&'app mut R>, P, O) {
        (self.app, self.plan, self.op)
    }

    /// Projects the pinned owner while preserving the selection and operation proofs.
    pub fn map_owner<U>(
        self,
        map: impl FnOnce(pin::Pin<&'app mut R>) -> pin::Pin<&'app mut U>,
    ) -> Call<'app, U, P, O> {
        Call {
            app: map(self.app),
            plan: self.plan,
            op: self.op,
        }
    }
}

impl<R, O> Call<'_, R, convert::Infallible, O> {
    /// Consumes an invocation whose selection proof has no inhabitants.
    pub fn into_never(self) -> ! {
        match self {}
    }
}

/// One complete coordinator count classified at installation.
#[derive(Clone, Copy, Debug)]
pub enum Count {
    Bounded(U32<0, { schedule::LIMIT as u32 }>),
    Excess(NonZero<usize>),
}

impl Count {
    pub const fn new(value: usize) -> Self {
        match U32::from_usize(value) {
            Some(value) => Self::Bounded(value),
            None => Self::Excess(NonZero::<usize>::MIN.saturating_add(value - 1)),
        }
    }

    pub fn sum<const N: usize>(values: [usize; N]) -> Option<Self> {
        values
            .into_iter()
            .try_fold(0usize, |count, value| count.checked_add(value))
            .map(Self::new)
    }

    pub const fn get(self) -> usize {
        match self {
            Self::Bounded(value) => value.into_usize(),
            Self::Excess(value) => value.get(),
        }
    }

    pub(in crate::executor::raw) fn admit(self) -> Result<(), executor::InstallError> {
        match self {
            Self::Bounded(_) => Ok(()),
            Self::Excess(count) => Err(executor::InstallError::Coordinators {
                count,
                limit: schedule::LIMIT,
            }),
        }
    }
}

/// Coordinator shares prepared once for one installed application.
#[repr(transparent)]
pub struct Weights<const N: usize> {
    values: [U32<0, { schedule::LIMIT as u32 }>; N],
}

impl<const N: usize> Weights<N> {
    pub const ZERO: Self = Self {
        values: [U32::exact::<0>(); N],
    };

    pub fn prepare(&mut self, values: [usize; N]) -> Option<Count> {
        let count = Count::sum(values)?;
        if let Count::Bounded(_) = count {
            self.values = values.map(|value| {
                // SAFETY: the admitted sum bounds each nonnegative share and its representation.
                unsafe { bounded::raw::Proof::assume(value as u32) }.retain()
            });
        }
        Some(count)
    }

    pub fn values(&self) -> [usize; N] {
        self.values.map(U32::into_usize)
    }
}

pub trait Finishes<'d, R = Self> {
    fn call<'app, 'finish, 'owner, 'epoch>(
        finish: Finish<'app, 'finish, 'owner, 'epoch, 'd, R>,
    ) -> Finish<'app, 'finish, 'owner, 'epoch, 'd, R>;
}

/// Installation traversal carrying its exact pinned root and owner proof.
#[must_use]
pub struct Install<'app, 'owner, 'd, R> {
    app: pin::Pin<&'app mut R>,
    install: life::Install<'owner, 'd>,
}

/// Shutdown traversal carrying its exact pinned root and driver turn.
#[must_use]
pub struct Shutdown<'app, 'turn, 'driver, 'owner, 'd: 'owner, R> {
    app: pin::Pin<&'app mut R>,
    turn: schedule::Turn<'turn, 'd>,
    driver: retained::Context<'driver, 'owner, 'd>,
}

/// Final traversal carrying installation and post-quiescence proofs together.
#[must_use]
pub struct Finish<'app, 'finish, 'owner, 'epoch, 'd, R> {
    app: pin::Pin<&'app mut R>,
    install: life::Install<'owner, 'd>,
    finish: &'finish mut finish::Finish<'epoch, 'd>,
}

impl<'app, 'owner, 'd, R> Install<'app, 'owner, 'd, R> {
    pub fn new(app: pin::Pin<&'app mut R>, install: life::Install<'owner, 'd>) -> Self {
        Self { app, install }
    }

    pub fn into_parts(self) -> (pin::Pin<&'app mut R>, life::Install<'owner, 'd>) {
        (self.app, self.install)
    }

    pub fn into_install(self) -> life::Install<'owner, 'd> {
        self.install
    }
}

impl<'app, 'turn, 'driver, 'owner, 'd: 'owner, R> Shutdown<'app, 'turn, 'driver, 'owner, 'd, R> {
    pub fn new(
        app: pin::Pin<&'app mut R>,
        turn: schedule::Turn<'turn, 'd>,
        driver: retained::Context<'driver, 'owner, 'd>,
    ) -> Self {
        Self { app, turn, driver }
    }

    pub fn into_parts(
        self,
    ) -> (
        pin::Pin<&'app mut R>,
        schedule::Turn<'turn, 'd>,
        retained::Context<'driver, 'owner, 'd>,
    ) {
        (self.app, self.turn, self.driver)
    }

    pub fn into_drive(
        self,
    ) -> (
        schedule::Turn<'turn, 'd>,
        retained::Context<'driver, 'owner, 'd>,
    ) {
        (self.turn, self.driver)
    }

    pub(crate) fn into_driver(self) -> driver::Context<'driver, 'd> {
        self.driver.into_driver()
    }
}

impl<'app, 'finish, 'owner, 'epoch, 'd, R> Finish<'app, 'finish, 'owner, 'epoch, 'd, R> {
    pub fn new(
        app: pin::Pin<&'app mut R>,
        install: life::Install<'owner, 'd>,
        finish: &'finish mut finish::Finish<'epoch, 'd>,
    ) -> Self {
        Self {
            app,
            install,
            finish,
        }
    }

    pub fn into_parts(
        self,
    ) -> (
        pin::Pin<&'app mut R>,
        life::Install<'owner, 'd>,
        &'finish mut finish::Finish<'epoch, 'd>,
    ) {
        (self.app, self.install, self.finish)
    }

    pub fn into_finish(
        self,
    ) -> (
        life::Install<'owner, 'd>,
        &'finish mut finish::Finish<'epoch, 'd>,
    ) {
        (self.install, self.finish)
    }
}

struct Drive<'driver, 'turn, 'root, 'd: 'root> {
    turn: schedule::Turn<'turn, 'd>,
    driver: retained::Context<'driver, 'root, 'd>,
}

impl<'driver, 'turn, 'root, 'd: 'root> Drive<'driver, 'turn, 'root, 'd> {
    const fn new(
        turn: schedule::Turn<'turn, 'd>,
        driver: retained::Context<'driver, 'root, 'd>,
    ) -> Self {
        Self { turn, driver }
    }

    fn reborrow(&mut self) -> Drive<'_, '_, 'root, 'd> {
        Drive {
            turn: self.turn.reborrow(),
            driver: self.driver.reborrow(),
        }
    }

    fn turn(&self) -> schedule::Turn<'_, 'd> {
        self.turn.reborrow()
    }

    fn driver(&mut self) -> retained::Context<'_, 'root, 'd> {
        self.driver.reborrow()
    }

    fn into_parts(
        self,
    ) -> (
        schedule::Turn<'turn, 'd>,
        retained::Context<'driver, 'root, 'd>,
    ) {
        (self.turn, self.driver)
    }

    fn into_driver(self) -> retained::Context<'driver, 'root, 'd> {
        self.driver
    }
}

const fn assert_layout() {
    assert!(
        mem::size_of::<Drive<'static, 'static, 'static, 'static>>() == 2 * mem::size_of::<usize>()
    );
    assert!(
        mem::align_of::<Drive<'static, 'static, 'static, 'static>>() == mem::align_of::<usize>()
    );
    assert!(mem::size_of::<Install<'static, 'static, 'static, ()>>() == mem::size_of::<usize>());
    assert!(
        mem::size_of::<Shutdown<'static, 'static, 'static, 'static, 'static, ()>>()
            == 3 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Finish<'static, 'static, 'static, 'static, 'static, ()>>()
            == 2 * mem::size_of::<usize>()
    );
}

const _: () = assert_layout();

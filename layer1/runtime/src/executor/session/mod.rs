use std::{mem, result::Result as StdResult, time};

use dope_core::{
    Errno,
    driver::{life, schedule::clock},
    io::Event,
};

use crate::executor::{InstallError, drive};

mod access;
mod app;
mod error;
mod lent;
mod manual;
pub(crate) mod run;
mod scope;

pub use access::Access;
pub use app::App;
pub use error::Error;
/// Session completion preserving output when quiescence fails.
pub type Result<'report, T> = std::result::Result<T, Error<'report, T>>;

pub use drive::block::Fixed;
pub(crate) use lent::Lent;
pub use manual::Manual;
pub use scope::Scope;

/// Exact runtime failures borrowed independently of retained application output.
#[derive(Debug)]
pub enum Cause<'a> {
    Drive(&'a drive::Error),
    Quiesce {
        drive: &'a drive::Error,
        disarm: Option<Errno>,
    },
    Install {
        cause: &'a InstallError,
        disarm: Option<Errno>,
    },
}

#[derive(Debug)]
pub enum Kind<'a, T> {
    Drive(&'a drive::Error),
    Quiesce {
        output: T,
        drive: &'a drive::Error,
        disarm: Option<Errno>,
    },
    Install {
        cause: &'a InstallError,
        disarm: Option<Errno>,
    },
}

impl<'a, T> Kind<'a, T> {
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> Kind<'a, U> {
        match self {
            Self::Drive(error) => Kind::Drive(error),
            Self::Quiesce {
                output,
                drive,
                disarm,
            } => Kind::Quiesce {
                output: map(output),
                drive,
                disarm,
            },
            Self::Install { cause, disarm } => Kind::Install { cause, disarm },
        }
    }
}

pub(crate) struct Core<'a, 'd, Q> {
    pub(crate) scope: life::Scope<'a, 'd>,
    pub(super) pending: Option<Event<'d>>,
    lifecycle: &'a Q,
}

impl<'a, 'd, Q> Core<'a, 'd, Q> {
    pub(super) fn new(scope: life::Scope<'a, 'd>, source: &'a Q) -> Self {
        Self {
            scope,
            pending: None,
            lifecycle: source,
        }
    }

    fn admit(&self, start: Option<clock::Deadline<'d>>) -> StdResult<clock::Sample, drive::Error> {
        let now = start.map_or_else(time::Instant::now, clock::Deadline::into_raw);
        self.check_time(now)?;
        Ok(clock::Sample::new(now))
    }

    fn check_time(&self, now: time::Instant) -> StdResult<(), drive::Error> {
        let current = self.scope.driver_ref().clock().now().into_raw();
        if now < current {
            return Err(drive::Error::regression(now, current));
        }
        Ok(())
    }
}

const _: () = {
    assert!(
        mem::size_of::<Core<'static, 'static, ()>>()
            == mem::size_of::<life::Scope<'static, 'static>>()
                + mem::size_of::<Option<Event<'static>>>()
                + mem::size_of::<usize>()
    );
};

/// Owns exact diagnostics after an application has relinquished its session.
#[derive(Debug, Default)]
pub struct Completion {
    quiesce: Option<drive::Error>,
}

impl Completion {
    pub const fn new() -> Self {
        Self { quiesce: None }
    }

    pub(crate) fn failed<T>(
        &mut self,
        output: T,
        drive: drive::Error,
        disarm: Option<Errno>,
    ) -> Error<'_, T> {
        let drive = self.quiesce.insert(drive);
        Error::Quiesce {
            output,
            drive,
            disarm,
        }
    }
}

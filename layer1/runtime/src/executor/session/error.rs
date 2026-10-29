use dope_core::Errno;

use crate::executor::{InstallError, drive, session};

/// Application output and the failure observed while releasing its session.
#[derive(Debug)]
pub enum Error<'report, T> {
    Drive(drive::Error),
    Quiesce {
        output: T,
        drive: &'report drive::Error,
        disarm: Option<Errno>,
    },
    Install {
        cause: InstallError,
        disarm: Option<Errno>,
    },
}

impl<T> Error<'_, T> {
    /// Borrows the application result retained through a later session failure.
    pub fn output(&self) -> Option<&T> {
        match self {
            Self::Quiesce { output, .. } => Some(output),
            Self::Drive(_) | Self::Install { .. } => None,
        }
    }

    /// Borrows the failure without moving or formatting application output.
    pub fn cause(&self) -> session::Cause<'_> {
        match self {
            Self::Drive(error) => session::Cause::Drive(error),
            Self::Quiesce { drive, disarm, .. } => session::Cause::Quiesce {
                drive,
                disarm: *disarm,
            },
            Self::Install { cause, disarm } => session::Cause::Install {
                cause,
                disarm: *disarm,
            },
        }
    }

    pub fn kind(&self) -> session::Kind<'_, &T> {
        match self {
            Self::Drive(error) => session::Kind::Drive(error),
            Self::Quiesce {
                output,
                drive,
                disarm,
            } => session::Kind::Quiesce {
                output,
                drive,
                disarm: *disarm,
            },
            Self::Install { cause, disarm } => session::Kind::Install {
                cause,
                disarm: *disarm,
            },
        }
    }
}

impl<T> From<drive::Error> for Error<'_, T> {
    fn from(error: drive::Error) -> Self {
        Self::Drive(error)
    }
}

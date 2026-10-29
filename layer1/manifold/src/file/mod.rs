pub mod appender;
mod locked;

use dope_core::{
    Errno,
    io::fs::{self, metadata},
};
pub use locked::Locked;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockError {
    Lock(Errno),
    Metadata(Errno),
    NotRegular(fs::Kind),
    Length(metadata::Negative),
}

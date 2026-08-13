pub mod affinities;
pub mod panics;
mod sealed;

use std::{fmt, process};

pub use sealed::TrackingAlloc;

pub(crate) trait Outcome<T> {
    fn or_abort(self, context: &str) -> T;
}

impl<T, E: fmt::Display> Outcome<T> for Result<T, E> {
    fn or_abort(self, context: &str) -> T {
        match self {
            Ok(value) => value,
            Err(error) => {
                eprintln!("dope-test {context}: {error}");
                process::abort()
            }
        }
    }
}

impl<T> Outcome<T> for Option<T> {
    fn or_abort(self, context: &str) -> T {
        match self {
            Some(value) => value,
            None => {
                eprintln!("dope-test {context}: required value was absent");
                process::abort()
            }
        }
    }
}

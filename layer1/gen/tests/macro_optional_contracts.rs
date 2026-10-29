//! A reexport of the standard Option retains the optional owner's identity.
//!
//! ```
//! use engine::{core::driver::route::KeyTag, manifold::timing::interval::Interval};
//! use engine::runtime::control;
//! mod choice { pub use core::option::Option; }
//! #[pin_project::pin_project]
//! #[derive(engine::Application)]
//! #[dispatcher(crate = ::engine)]
//! #[coordinate]
//! struct App<'d> {
//!     #[pin]
//!     #[manifold(optional, control)]
//!     timer: choice::Option<Interval<'d, KeyTag<1>>>,
//! }
//! impl<'d> App<'d> {
//!     fn coordinate(_: app_coord::Fields<'_, 'd>) -> control::Flow {
//!         control::Flow::Idle
//!     }
//! }
//! # fn main() {}
//! ```
//!
//! Safe same-named wrappers can change projected presence and cannot supply the proof.
//!
//! ```compile_fail,E0308
//! # mod common;
//! use common::optional::Option;
//! use engine::{core::driver::route::KeyTag, manifold::timing::interval::Interval};
//! use engine::runtime::control;
//! #[pin_project::pin_project]
//! #[derive(engine::Application)]
//! #[dispatcher(crate = ::engine)]
//! #[coordinate]
//! struct App<'d> {
//!     #[pin]
//!     #[manifold(optional, control)]
//!     timer: Option<Interval<'d, KeyTag<1>>>,
//! }
//! impl<'d> App<'d> {
//!     fn coordinate(_: app_coord::Fields<'_, 'd>) -> control::Flow {
//!         control::Flow::Idle
//!     }
//! }
//! # fn main() {}
//! ```

pub mod common;

use std::env;

use dope_test::checks::{Error, Suite};

#[test]
fn requires_standard_option() -> Result<(), Error> {
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &[
            "engine=dope",
            "fibers=dope-fiber",
            "dope-test",
            "pin-project",
        ],
    );
    let exe = env::current_exe()?;
    let status = suite.try_build(&exe, env!("CARGO_PKG_NAME"))?.status()?;
    assert!(status.success(), "build original library: {status}");
    assert_eq!(
        suite.try_errors(&exe, include_str!("macro_optional_contracts.rs"))?,
        1
    );
    let status = suite.try_rustdoc(&exe)?.status()?;
    assert!(status.success(), "compile-time contracts: {status}");
    Ok(())
}

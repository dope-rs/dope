//! The backing scope remains invariant before an arena or batches are constructed.
//!
//! ```
//! use dope_fiber::reply::Storage;
//! fn preserve<'a, 'c, 'r>(storage: Storage<'a, 'c, 'r, ()>) -> Storage<'a, 'c, 'r, ()> {
//!     storage
//! }
//! ```
//!
//! ```compile_fail
//! use dope_fiber::reply::Storage;
//! fn shorten<'short, 'long: 'short, 'c, 'r>(
//!     storage: Storage<'long, 'c, 'r, ()>,
//! ) -> Storage<'short, 'c, 'r, ()> {
//!     storage
//! }
//! ```
//!
//! Independent factories cannot share an arena domain by shortening their storage.
//!
//! ```compile_fail
//! use dope_fiber::reply::{Storage, layout::Layout};
//! use o3::base::cell::region::{Identity, Proof};
//! fn batches<'a, 'left, 'right, 'r>(
//!     left: Storage<'a, 'left, 'r, ()>,
//!     right: Storage<'a, 'right, 'r, ()>,
//! ) {
//!     left.try_batches::<(), (), _>(|first, _| {
//!         right.try_batches::<(), (), _>(|second, _| {
//!             let mut domain = first.domain();
//!             domain = second.domain();
//!             let _ = domain;
//!         }).expect("right arena");
//!     }).expect("left arena");
//! }
//! fn merge<'r>(layout: Layout, proof: Proof<Identity<'r>>) {
//!     Storage::try_with(layout, proof, |left| {
//!         Storage::try_with(layout, proof, |right| batches(left, right))
//!             .expect("right storage");
//!     }).expect("left storage");
//! }
//! ```

use std::env;

use dope_test::checks::{Error, Suite};

#[test]
fn preserves_domain() -> Result<(), Error> {
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &["dope-fiber", "o3", "dope-test"],
    );
    let exe = env::current_exe()?;
    assert!(
        suite
            .try_build(&exe, env!("CARGO_PKG_NAME"))?
            .status()?
            .success()
    );
    assert!(suite.try_rustdoc(&exe)?.status()?.success());
    Ok(())
}

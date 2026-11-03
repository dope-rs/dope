//! A borrowed arena cannot be moved out of its retained owner.
//!
//! ```compile_fail,E0507
//! use dope_fiber::reply::Arena;
//! fn take<'arena, 'credits, 'region>(arena: &Arena<'arena, 'credits, 'region, ()>) -> Arena<'arena, 'credits, 'region, ()> {
//!     *arena
//! }
//! ```

//! One selection permits inspection and insertion, then releases its region token.
//!
//! ```
//! use dope_fiber::reply::{Lane, usage};
//! use o3::base::cell::region::{Identity, Token};
//! fn insert<'region, D: usage::Footprint>(
//!     lane: Lane<'_, '_, '_, 'region, D, (), u8>,
//!     token: &mut Token<Identity<'region>>,
//!     value: D,
//! ) {
//!     let mut selected = lane.select(token);
//!     let tag = selected.tag().copied();
//!     let front = selected.front();
//!     let result = selected.try_push(value);
//!     let _ = (tag, front, result, lane.depth(token));
//! }
//! ```
//!
//! A retained selection excludes reentry through its region token.
//!
//! ```compile_fail,E0499
//! use dope_fiber::reply::Lane;
//! use o3::base::cell::region::{Identity, Token};
//! fn reenter<'region, D>(
//!     lane: Lane<'_, '_, '_, 'region, D, (), u8>,
//!     token: &mut Token<Identity<'region>>,
//! ) {
//!     let selected = lane.select(token);
//!     lane.complete(token);
//!     let _ = selected.front();
//! }
//! ```
//!
//! A borrowed tag prevents consumption of its selected owner.
//!
//! ```compile_fail,E0505
//! use dope_fiber::reply::{Lane, usage};
//! use o3::base::cell::region::{Identity, Token};
//! fn consume<'region, D: usage::Footprint>(
//!     lane: Lane<'_, '_, '_, 'region, D, (), u8>,
//!     token: &mut Token<Identity<'region>>,
//!     value: D,
//! ) {
//!     let mut selected = lane.select(token);
//!     let tag = selected.tag();
//!     let _ = selected.try_push(value);
//!     assert!(tag.is_some());
//! }
//! ```

//! A slot can shorten its owner loan while retaining the exact arena.
//!
//! ```
//! use dope_fiber::reply::Slot;
//! fn loan<'a, 'short, 'long: 'short, 'arena, 'credits, 'region>(
//!     slot: Slot<'a, 'long, 'arena, 'credits, 'region, ()>,
//! ) -> Slot<'a, 'short, 'arena, 'credits, 'region, ()> {
//!     slot
//! }
//! ```
//!
//! The exclusive owner borrow keeps the arena lifetime invariant.
//!
//! ```compile_fail
//! use dope_fiber::reply::Slot;
//! fn arena<'a, 's, 'short, 'long: 'short, 'credits, 'region>(
//!     slot: Slot<'a, 's, 'long, 'credits, 'region, ()>,
//! ) -> Slot<'a, 's, 'short, 'credits, 'region, ()> {
//!     slot
//! }
//! ```

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

#[cfg(test)]
use dope_test::checks::{Error, Suite};

#[test]
fn preserves_ownership() -> Result<(), Error> {
    let suite = Suite::new(env!("CARGO_MANIFEST_DIR"), file!(), &["dope-fiber", "o3"]);
    let exe = env::current_exe().expect("current test executable");
    let output = suite
        .try_build(&exe, "dope-fiber")?
        .output()
        .expect("run cargo");
    let suite = suite.try_collect(&exe, output)?;
    let output = compile(
        &mut suite.compiler(),
        include_str!("common/compile_resume_unpin.rs"),
    );
    let message = String::from_utf8(output.stderr).expect("compiler diagnostics");
    assert!(
        !output.status.success()
            && message.contains("error[E0277]")
            && message.contains("PhantomPinned")
            && message.contains("Unpin"),
        "consuming resume requires Unpin: {message}"
    );
    assert_eq!(
        suite.try_errors(include_str!("reply_ownership_contracts.rs"), compile)?,
        3
    );
    let status = suite.try_rustdoc()?.status().expect("run rustdoc");
    assert!(status.success(), "compile-time contracts: {status}");
    Ok(())
}

fn compile(command: &mut std::process::Command, source: &str) -> std::process::Output {
    let mut child = command.spawn().expect("start compiler");
    let written = child
        .stdin
        .take()
        .map(|mut input| std::io::Write::write_all(&mut input, source.as_bytes()));
    let output = child.wait_with_output().expect("reap compiler");
    written
        .expect("compiler stdin")
        .expect("write compiler source");
    output
}

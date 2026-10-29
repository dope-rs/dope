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

use std::env;

use dope_test::checks::{Error, Suite};

#[test]
fn preserves_borrows() -> Result<(), Error> {
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &["dope-fiber", "o3", "dope-test"],
    );
    let exe = env::current_exe()?;
    let status = suite.try_build(&exe, env!("CARGO_PKG_NAME"))?.status()?;
    assert!(status.success(), "build original library: {status}");
    assert_eq!(
        suite.try_errors(&exe, include_str!("reply_selection_contracts.rs"))?,
        3
    );
    let status = suite.try_rustdoc(&exe)?.status()?;
    assert!(status.success(), "compile-time contracts: {status}");
    Ok(())
}

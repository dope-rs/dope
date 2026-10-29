//! A port rejects connections from another route.
//!
//! ```compile_fail,E0308
//! use dope_manifold::client::{conn, port::Port};
//! use o3::base::cell::region;
//!
//! fn cross_route<'d>(
//!     port: &Port<'_, 'd, &'static [u8], dope_core::driver::route::KeyTag<1>>,
//!     region: &mut region::Token<region::Identity<'d>>,
//!     connection: conn::Id<'d, dope_core::driver::route::KeyTag<2>>,
//! ) {
//!     port.try_enqueue(region, connection, b"payload");
//! }
//! ```
//!
//! Route identity and connection capacity are independent contracts.
//!
//! ```compile_fail,E0277
//! use dope_manifold::client::session::Target;
//!
//! fn require<'d, N: Target<'d, dope_core::driver::route::KeyTag<8>, 1>>() {}
//!
//! fn wrong_route<'d, N: Target<'d, dope_core::driver::route::KeyTag<7>, 1>>() {
//!     require::<'d, N>();
//! }
//! ```
//!
//! ```compile_fail,E0277
//! use dope_manifold::client::session::Target;
//!
//! fn require<'d, N: Target<'d, dope_core::driver::route::KeyTag<7>, 2>>() {}
//!
//! fn unsupported_capacity<'d, N: Target<'d, dope_core::driver::route::KeyTag<7>, 1>>() {
//!     require::<'d, N>();
//! }
//! ```
//!
//! An unpublished attempt cannot issue a completion receipt.
//!
//! ```compile_fail,E0599
//! use dope_manifold::client::attempt::queue::mailbox::Vacant;
//! use dope_net::tcp::Tcp;
//!
//! fn unpublished<'source, 'd>(vacant: &Vacant<'source, 'd, Tcp, (), ()>) {
//!     vacant.receipt();
//! }
//! ```
//!
//! Attempts retain validated options and exact-source completion receipts.
//!
//! ```compile_fail,E0308
//! use std::net::SocketAddr;
//! use dope_manifold::client::attempt::StreamTarget;
//! use dope_net::tcp::{StreamConfig, Tcp};
//!
//! fn unchecked(addr: SocketAddr) {
//!     StreamTarget::<Tcp>::new(addr, StreamConfig::default());
//! }
//! ```
//!
//! ```compile_fail,E0308
//! use dope_manifold::client::attempt::{Id, queue::mailbox::Domain};
//! use dope_net::tcp::Tcp;
//!
//! fn settle_key<'d>(domain: &Domain<'d, Tcp, (), ()>, key: Id<'d>) {
//!     domain.try_settle(&key, ());
//! }
//! ```
//!
//! Connection identities preserve their route and transport family.
//!
//! ```compile_fail,E0308
//! use dope_core::driver::route::KeyTag;
//! use dope_manifold::client::conn::Id;
//!
//! fn cross_route<'d>(id: Id<'d, KeyTag<1>>) -> Id<'d, KeyTag<2>> {
//!     id
//! }
//! ```
//!
//! ```compile_fail,E0308
//! use dope_core::driver::route::KeyTag;
//! use dope_manifold::{client, network::listener};
//!
//! fn cross_family<'d>(id: listener::conn::Id<'d, KeyTag<1>>)
//!     -> client::conn::Id<'d, KeyTag<1>>
//! {
//!     id
//! }
//! ```
//!
//! Retirement lifetimes preserve their connector route.
//!
//! ```compile_fail,E0308
//! use dope_core::driver::route::KeyTag;
//! use dope_manifold::client::conn;
//!
//! fn reroute<'d>(
//!     lifetime: conn::Lifetime<'d, KeyTag<0>>,
//! ) -> conn::Lifetime<'d, KeyTag<1>> {
//!     lifetime
//! }
//! ```
//!
//! Endpoint snapshots reject unsupported inline capacity.
//!
//! ```compile_fail,E0080
//! use dope_manifold::service::{Revision, Snapshot};
//!
//! Snapshot::<(), 17>::empty(Revision::INITIAL);
//! ```
//!
//! Raw retained owners require exact-root installation before driving.
//!
//! ```compile_fail,E0133
//! use std::pin::Pin;
//! use dope_core::driver::schedule;
//! use dope_manifold::app::dispatch::raw::{Context, Manifold};
//!
//! fn bypass_install<'d, M: Manifold<'d>>(
//!     owner: Pin<&mut M>,
//!     turn: schedule::Turn<'_, 'd>,
//!     driver: &mut Context<'_, '_, 'd, M::PrePark>,
//! ) {
//!     M::pre_park(owner, turn, driver);
//! }
//! ```

use std::{env, io::Write, process::Stdio};

use dope_test::checks::{Error, Suite};

#[test]
fn discard_requires_enabled() -> Result<(), Error> {
    let exe = env::current_exe()?;
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &["dope-core", "dope-net", "dope-manifold", "o3"],
    );
    let status = suite.try_build(&exe, env!("CARGO_PKG_NAME"))?.status()?;
    assert!(status.success());

    assert_eq!(
        suite.try_errors(&exe, include_str!("listener_discard_contracts.rs"))?,
        11
    );

    for (source, expected) in [
        (
            include_str!("common/compile_listener_discard_enabled.rs"),
            &[] as &[&str],
        ),
        (
            include_str!("common/compile_listener_discard_unproven.rs"),
            &["error[E0271]: type mismatch resolving"],
        ),
        (include_str!("common/compile_codec_accepted.rs"), &[]),
        (include_str!("common/compile_codec_slot_accepted.rs"), &[]),
        (
            include_str!("common/compile_codec_slot_substitute.rs"),
            &["lifetime may not live long enough"],
        ),
        (
            include_str!("common/compile_codec_slot_vacant.rs"),
            &["error[E0308]"],
        ),
        (
            include_str!("common/compile_codec_input.rs"),
            &["lifetime may not live long enough"],
        ),
        (
            include_str!("common/compile_codec_error.rs"),
            &["lifetime may not live long enough"],
        ),
        (
            include_str!("common/compile_codec_direct_retry.rs"),
            &["but it returns `Direct`"],
        ),
        (
            include_str!("common/compile_codec_delivery_retry.rs"),
            &["but it returns `Delivery<()>`"],
        ),
        (
            include_str!("common/compile_codec_outcome.rs"),
            &["error[E0277]", "Sealed"],
        ),
        (
            include_str!("common/compile_datagram_escape.rs"),
            &["lifetime may not live long enough"],
        ),
        (
            include_str!("common/compile_datagram_substitute.rs"),
            &["lifetime may not live long enough"],
        ),
    ] {
        let mut compiler = suite
            .try_compiler(&exe)?
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        compiler
            .stdin
            .take()
            .expect("compiler stdin")
            .write_all(source.as_bytes())?;
        let output = compiler.wait_with_output()?;
        let diag = String::from_utf8(output.stderr).expect("diagnostic UTF-8");
        assert!(
            output.status.success() == expected.is_empty()
                && expected.iter().all(|part| diag.contains(part)),
            "{source}\n{diag}"
        );
    }
    Ok(())
}

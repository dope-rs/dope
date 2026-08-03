mod checks;
mod fiber;
mod file;
mod harness;
mod peer;
mod rt;
mod scenario;

use std::time::Duration;

pub use checks::{
    CountDrop, OrderedDrop, TrackingAlloc, allocations_during, assert_panics_with, assert_unwinds,
    counter, expect_abort, not_send, not_sync, not_unpin, require_send, respawn_self,
};
pub use fiber::{Gate, drive, poll_ready, poll_with_slot, pump_events, run_until, with_context};
pub use file::TempFile;
pub use harness::Harness;
pub use peer::{
    connect, connect_with_read_timeout, hold_connections, pattern, read_all, request_reply,
    reserve_addr, spawn_peer,
};
pub use rt::{
    Plain, TcpConfig, Wired, drain_tokens, exec, exec_for, file_exec, listener_exec, open_listener,
    quic_exec, tcp_host, throughput_cfg, tok, with_driver, with_session, with_session_for,
    with_session_timer_slots,
};
pub use scenario::{ListenerHost, ManifoldHost, TcpCase};

pub const GUARD: Duration = Duration::from_secs(5);

#[doc(hidden)]
pub mod __private {
    pub use dope;
    pub use dope_net;
    pub use o3::cell::BrandCell;
}

/// Runs one TCP listener scenario with bounded runtime, peer, and shutdown helpers.
///
/// The short form uses the identity wire and the default TCP listener config:
///
/// ```ignore
/// dope_test::tcp_case! {
///     max_connections: 64,
///     app: app,
///     |case| {
///         let peer = case.request_reply(b"ping".to_vec());
///         case.until(&gate, 1);
///         assert_eq!(peer.join().unwrap(), b"pong");
///     }
/// }
/// ```
#[macro_export]
macro_rules! tcp_case {
    (
        id: $id:literal,
        max_connections: $max_connections:expr,
        transport: $transport:expr,
        env: $env:ty,
        app: $app:expr,
        |$case:ident| $body:block $(,)?
    ) => {{
        let (__dope_exec, __dope_config) = $crate::tcp_host($max_connections, $transport);
        __dope_exec.enter(|mut __dope_session| {
            let __dope_hash = __dope_session
                .seed()
                .derive($crate::__private::dope::hash::domain::ACCEPT)
                .state();
            let (__dope_listener, __dope_addr) = $crate::open_listener::<$id, _, $env>(
                $app,
                __dope_config,
                __dope_hash,
                __dope_session.storage(),
                &mut __dope_session.driver_access(),
            );
            let __dope_host = ::std::pin::pin!($crate::__private::BrandCell::new(
                $crate::ListenerHost::new(__dope_listener),
            ));
            let mut $case =
                $crate::TcpCase::new(&mut __dope_session, __dope_host.as_ref(), __dope_addr);
            $body
        })
    }};
    (
        max_connections: $max_connections:expr,
        transport: $transport:expr,
        env: $env:ty,
        app: $app:expr,
        |$case:ident| $body:block $(,)?
    ) => {
        $crate::tcp_case! {
            id: 0,
            max_connections: $max_connections,
            transport: $transport,
            env: $env,
            app: $app,
            |$case| $body
        }
    };
    (
        max_connections: $max_connections:expr,
        transport: $transport:expr,
        app: $app:expr,
        |$case:ident| $body:block $(,)?
    ) => {
        $crate::tcp_case! {
            id: 0,
            max_connections: $max_connections,
            transport: $transport,
            env: $crate::Plain,
            app: $app,
            |$case| $body
        }
    };
    (
        max_connections: $max_connections:expr,
        app: $app:expr,
        |$case:ident| $body:block $(,)?
    ) => {
        $crate::tcp_case! {
            max_connections: $max_connections,
            transport: $crate::__private::dope_net::tcp::listener::Config::default(),
            app: $app,
            |$case| $body
        }
    };
}

/// Runs one static TCP connector scenario against an existing loopback address.
#[macro_export]
macro_rules! connector_case {
    (
        id: $id:literal,
        max_connections: $max_connections:expr,
        address: $address:expr,
        backoff: $backoff:expr,
        $(timer_slots: $timer_slots:expr,)?
        env: $env:ty,
        app: $app:expr,
        |$case:ident| $body:block $(,)?
    ) => {{
        let __dope_addr = $address;
        let __dope_config = $crate::__private::dope::driver::Config::for_tcp_profile::<
            $crate::__private::dope::runtime::profile::Throughput,
        >($max_connections);
        $(let __dope_config = {
            let mut __dope_config = __dope_config;
            __dope_config.timer_slots = $timer_slots;
            __dope_config
        };)?
        $crate::__private::dope::runtime::executor::Executor::new(__dope_config)
            .expect("executor")
            .with_storage($crate::__private::dope_net::link::egress::storage::Storage::default())
            .enter(|mut __dope_session| {
                let __dope_seed = $crate::__private::dope::hash::Seed::new([1, 2]).state();
                let __dope_dialer =
                    $crate::__private::dope::manifold::connector::source::health::Static::<
                        $crate::__private::dope_net::tcp::Tcp,
                    >::new(vec![__dope_addr], $backoff, __dope_seed);
                let __dope_connector = $crate::__private::dope::manifold::connector::core::Core::<
                    $id,
                    _,
                    _,
                    $env,
                >::with_app(
                    $app,
                    __dope_dialer,
                    $max_connections,
                    __dope_session.storage(),
                    &mut __dope_session.driver_access(),
                )
                .expect("connector");
                let __dope_host = ::std::pin::pin!($crate::__private::BrandCell::new(
                    $crate::ManifoldHost::new(__dope_connector),
                ));
                let mut $case =
                    $crate::TcpCase::new(&mut __dope_session, __dope_host.as_ref(), __dope_addr);
                $body
            })
    }};
    (
        max_connections: $max_connections:expr,
        address: $address:expr,
        backoff: $backoff:expr,
        $(timer_slots: $timer_slots:expr,)?
        app: $app:expr,
        |$case:ident| $body:block $(,)?
    ) => {
        $crate::connector_case! {
            id: 0,
            max_connections: $max_connections,
            address: $address,
            backoff: $backoff,
            $(timer_slots: $timer_slots,)?
            env: $crate::Plain,
            app: $app,
            |$case| $body
        }
    };
}

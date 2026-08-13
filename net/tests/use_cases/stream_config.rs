use std::{io::ErrorKind, time::Duration};

use dope_core::io::socket::option::{Stream, StreamOptions};
use dope_net::{
    ListenerTransport, Transport,
    tcp::{self, FastOpenBacklog, Tcp},
    unix::{self, Unix},
};

fn error_kind<T>(result: std::io::Result<T>) -> ErrorKind {
    match result {
        Ok(_) => panic!("invalid stream configuration must be rejected"),
        Err(error) => error.kind(),
    }
}

#[test]
fn stream_buffers_reject_values_the_kernel_abi_cannot_represent() {
    let tcp = tcp::StreamConfig {
        recv_buffer_size: Some(usize::MAX),
        ..tcp::StreamConfig::default()
    };
    let unix = unix::StreamConfig {
        send_buffer_size: Some(usize::MAX),
        ..unix::StreamConfig::default()
    };

    assert_eq!(
        error_kind(Tcp::stream_options(tcp)),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        error_kind(Unix::stream_options(unix)),
        ErrorKind::InvalidInput
    );
}

#[test]
fn stream_options_reject_duplicate_typed_settings() {
    let duplicated: std::io::Result<StreamOptions> =
        [Some(Stream::NoDelay(true)), Some(Stream::NoDelay(false))].try_into();
    assert_eq!(error_kind(duplicated), ErrorKind::InvalidInput);
}

#[test]
fn tcp_fast_open_backlog_is_positive() {
    assert!(FastOpenBacklog::new(0).is_none());
    assert!(FastOpenBacklog::new(-1).is_none());
    assert_eq!(
        FastOpenBacklog::new(256).expect("positive backlog").get(),
        256
    );
}

#[test]
fn tcp_keep_alive_rejects_zero_and_overflow() {
    let zero_idle = tcp::StreamConfig {
        keep_alive_idle: Some(Duration::ZERO),
        ..tcp::StreamConfig::default()
    };
    let zero_retries = tcp::StreamConfig {
        keep_alive_retries: Some(0),
        ..tcp::StreamConfig::default()
    };
    let overflowing_idle = tcp::StreamConfig {
        keep_alive_idle: Some(Duration::MAX),
        ..tcp::StreamConfig::default()
    };

    for config in [zero_idle, zero_retries, overflowing_idle] {
        assert_eq!(
            error_kind(Tcp::stream_options(config)),
            ErrorKind::InvalidInput
        );
    }
}

#[test]
fn failed_listener_binds_do_not_exhaust_fixed_slots() {
    use std::{
        net::{Ipv4Addr, TcpListener},
        pin::pin,
    };

    use dope_core::driver::{Driver, settings};

    let occupied = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("occupied address");
    let addr = occupied.local_addr().expect("occupied address");
    let mut driver = pin!(
        Driver::new(settings::Config::for_quic_udp(2, 8).expect("driver config"))
            .expect("small fixed-file table")
    );

    crate::scope(driver.as_mut(), |mut scope| {
        let mut access = scope.context();
        for _ in 0..32 {
            let error =
                Tcp::bind_listener_slot(&mut access, &addr, 128, &tcp::ListenerConfig::default())
                    .expect_err("occupied address must reject bind");
            assert_ne!(error.kind(), ErrorKind::OutOfMemory);
        }
    });
}

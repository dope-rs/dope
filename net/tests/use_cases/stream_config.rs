use std::io::ErrorKind;
use std::time::Duration;

#[cfg(target_os = "linux")]
use dope_net::ListenerTransport;
use dope_net::Transport;
use dope_net::tcp::{self, Tcp};
use dope_net::unix::{self, Unix};

#[test]
fn stream_buffers_reject_values_the_kernel_abi_cannot_represent() {
    let tcp = tcp::stream::Config {
        recv_buffer_size: Some(usize::MAX),
        ..tcp::stream::Config::default()
    };
    let unix = unix::stream::Config {
        send_buffer_size: Some(usize::MAX),
        ..unix::stream::Config::default()
    };

    assert_eq!(
        Tcp::validate_stream_config(tcp).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        Unix::validate_stream_config(unix).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[test]
fn tcp_keep_alive_rejects_zero_and_overflow() {
    let zero_idle = tcp::stream::Config {
        keep_alive_idle: Some(Duration::ZERO),
        ..tcp::stream::Config::default()
    };
    let zero_retries = tcp::stream::Config {
        keep_alive_retries: Some(0),
        ..tcp::stream::Config::default()
    };
    let overflowing_idle = tcp::stream::Config {
        keep_alive_idle: Some(Duration::MAX),
        ..tcp::stream::Config::default()
    };

    for config in [zero_idle, zero_retries, overflowing_idle] {
        assert_eq!(
            Tcp::validate_stream_config(config).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn tcp_listener_options_reject_values_the_kernel_abi_cannot_represent() {
    use std::net::{Ipv4Addr, SocketAddr};

    use dope_test::with_driver;

    with_driver(|mut driver| {
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let config = tcp::listener::Config {
            fast_open_backlog: Some(u32::MAX),
            ..tcp::listener::Config::default()
        };
        let error = Tcp::bind_listener_slot(&mut driver, &addr, 128, &config).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    });
}

#[cfg(target_os = "linux")]
#[test]
fn failed_listener_binds_do_not_exhaust_fixed_slots() {
    use std::net::{Ipv4Addr, TcpListener};
    use std::pin::pin;

    use dope_core::driver::ext::DriverExt;
    use dope_core::driver::{self, Driver};

    let occupied = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("occupied address");
    let addr = occupied.local_addr().expect("occupied address");
    let mut driver =
        pin!(Driver::new(driver::Config::for_quic_udp(1, 8)).expect("small fixed-file table"));

    driver.as_mut().scope(|mut scope| {
        let mut access = scope.context();
        for _ in 0..32 {
            let error =
                Tcp::bind_listener_slot(&mut access, &addr, 128, &tcp::listener::Config::default())
                    .expect_err("occupied address must reject bind");
            assert_ne!(error.kind(), ErrorKind::OutOfMemory);
        }
    });
}

#[cfg(target_os = "linux")]
#[test]
fn linux_accepts_subunit_tcp_durations() {
    let config = tcp::stream::Config {
        keep_alive_idle: Some(Duration::from_nanos(1)),
        keep_alive_interval: Some(Duration::from_nanos(1)),
        user_timeout: Some(Duration::from_nanos(1)),
        ..tcp::stream::Config::default()
    };

    Tcp::validate_stream_config(config).unwrap();
}

#[cfg(target_os = "macos")]
#[test]
fn macos_rejects_linux_only_tcp_options() {
    for config in [
        tcp::stream::Config {
            quick_ack: Some(true),
            ..tcp::stream::Config::default()
        },
        tcp::stream::Config {
            user_timeout: Some(Duration::from_secs(1)),
            ..tcp::stream::Config::default()
        },
    ] {
        assert_eq!(
            Tcp::validate_stream_config(config).unwrap_err().kind(),
            ErrorKind::Unsupported
        );
    }
}

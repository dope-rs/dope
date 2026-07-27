use std::io::ErrorKind;
use std::time::Duration;

#[cfg(target_os = "linux")]
use dope_net::ListenerTransport;
use dope_net::Transport;
use dope_net::tcp::Tcp;
use dope_net::tcp::stream::Config as TcpConfig;
use dope_net::unix::Unix;
use dope_net::unix::stream::Config as UnixConfig;

#[test]
fn stream_buffers_reject_values_the_kernel_abi_cannot_represent() {
    let tcp = TcpConfig {
        recv_buffer_size: Some(usize::MAX),
        ..TcpConfig::default()
    };
    let unix = UnixConfig {
        send_buffer_size: Some(usize::MAX),
        ..UnixConfig::default()
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
    let zero_idle = TcpConfig {
        keep_alive_idle: Some(Duration::ZERO),
        ..TcpConfig::default()
    };
    let zero_retries = TcpConfig {
        keep_alive_retries: Some(0),
        ..TcpConfig::default()
    };
    let overflowing_idle = TcpConfig {
        keep_alive_idle: Some(Duration::MAX),
        ..TcpConfig::default()
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

    use dope_net::tcp::listener::Config as TcpListenerConfig;
    use dope_test::with_driver;

    with_driver(|mut driver| {
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let config = TcpListenerConfig {
            fast_open_backlog: Some(u32::MAX),
            ..TcpListenerConfig::default()
        };
        let error = Tcp::bind_listener_slot(&mut driver, &addr, 128, &config).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    });
}

#[cfg(target_os = "linux")]
#[test]
fn linux_accepts_subunit_tcp_durations() {
    let config = TcpConfig {
        keep_alive_idle: Some(Duration::from_nanos(1)),
        keep_alive_interval: Some(Duration::from_nanos(1)),
        user_timeout: Some(Duration::from_nanos(1)),
        ..TcpConfig::default()
    };

    Tcp::validate_stream_config(config).unwrap();
}

#[cfg(target_os = "macos")]
#[test]
fn macos_rejects_linux_only_tcp_options() {
    for config in [
        TcpConfig {
            quick_ack: Some(true),
            ..TcpConfig::default()
        },
        TcpConfig {
            user_timeout: Some(Duration::from_secs(1)),
            ..TcpConfig::default()
        },
    ] {
        assert_eq!(
            Tcp::validate_stream_config(config).unwrap_err().kind(),
            ErrorKind::Unsupported
        );
    }
}

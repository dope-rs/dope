use std::time::Duration;

use dope_core::driver::bootstrap::Bootstrap;
use dope_core::driver::completion::Completion;
use dope_core::driver::control::ContextControl;
use dope_core::io::socket::option::SocketOption;
use dope_test::with_driver;

#[test]
fn reserved_fd_guard_returns_its_fixed_slot() {
    with_driver(|mut driver| {
        let (first, _) = driver
            .bind_datagram_slot("127.0.0.1:0".parse().expect("first address"))
            .expect("first socket");
        let slot = first.index();
        drop(driver.guard(first));

        let (second, _) = driver
            .bind_datagram_slot("127.0.0.1:0".parse().expect("second address"))
            .expect("second socket");
        assert_eq!(second.index(), slot);
        drop(driver.guard(second));
    });
}

#[test]
fn setsockopt_completion_is_consumed() {
    with_driver(|mut driver| {
        let (socket, _) = driver
            .bind_datagram_slot("127.0.0.1:0".parse().expect("address"))
            .expect("bind datagram");

        driver
            .submit_option(
                &socket,
                SocketOption::new(libc::SOL_SOCKET, libc::SO_REUSEADDR, 1),
            )
            .expect("submit setsockopt");

        driver.wait(Some(Duration::from_secs(1))).expect("wait");
        let mut completions = [const { None }; 1];
        assert_eq!(driver.drain(&mut completions), 0);
    });
}

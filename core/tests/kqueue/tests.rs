use std::net::UdpSocket;
use std::time::Duration;

use dope_core::backend::Sqe;
use dope_core::driver::bootstrap::Bootstrap;
use dope_core::driver::completion::Completion;
use dope_core::driver::submission::Submission;
use dope_core::driver::token::{Epoch, SlotIndex, Token, kind};
use dope_core::io::Cqe;
use dope_test::with_driver;

fn open_fds() -> Vec<libc::c_int> {
    (0..4096)
        .filter(|fd| unsafe { libc::fcntl(*fd, libc::F_GETFD) } >= 0)
        .collect()
}

#[test]
fn close_retires_recv_before_raw_fd_reuse() {
    with_driver(|mut driver| {
        let before = open_fds();
        let (old, old_addr) = driver
            .bind_datagram_slot("127.0.0.1:0".parse().expect("old address"))
            .expect("old socket");
        let after_old = open_fds();
        let opened = after_old
            .iter()
            .copied()
            .filter(|fd| !before.contains(fd))
            .collect::<Vec<_>>();
        assert_eq!(opened.len(), 1);
        let reused = opened[0];
        let old_token = Token::new(1, SlotIndex::new(0), Epoch::INITIAL);
        driver
            .push(unsafe { Sqe::recv_multi(&old, 0, old_token) })
            .expect("arm old recv");

        let peer = UdpSocket::bind("127.0.0.1:0").expect("peer");
        peer.send_to(&[0x41; 32], old_addr).expect("send old");
        driver
            .wait(Some(Duration::from_secs(1)))
            .expect("receive old");

        drop(driver.guard(old));
        assert_eq!(unsafe { libc::fcntl(reused, libc::F_GETFD) }, -1);

        let (new, new_addr) = driver
            .bind_datagram_slot("127.0.0.1:0".parse().expect("new address"))
            .expect("new socket");
        assert!(unsafe { libc::fcntl(reused, libc::F_GETFD) } >= 0);
        let new_token = Token::new(2, SlotIndex::new(0), Epoch::INITIAL);
        driver
            .push(unsafe { Sqe::recv_multi(&new, 0, new_token) })
            .expect("arm new recv");
        peer.send_to(b"new", new_addr).expect("send new");
        driver
            .wait(Some(Duration::from_secs(1)))
            .expect("receive new");

        let mut completions = [Cqe::ZERO; 2];
        let n = driver.drain(&mut completions);
        assert_eq!(n, 1);
        assert_eq!(completions[0].route(), new_token.route());
        assert_eq!(completions[0].kind(), kind::RECV);

        drop(driver.guard(new));
    });
}

use std::net::SocketAddr;

use dope_core::io::datagram;
use dope_sys::io::socket::route::{Plan, selector::Selector};

pub const SELECTOR: Selector = Selector::fixed::<0, 0x80, 1, 10, 5, 9, 6, 16>();

pub fn is_available(
    result: &Result<datagram::Group, datagram::Error>,
    address: SocketAddr,
    plan: Plan,
) -> bool {
    if std::env::consts::OS == "macos" {
        assert_eq!(
            result.as_ref().err(),
            Some(&datagram::Error::Unsupported { address, plan })
        );
        return false;
    }
    // BPF map creation needs CAP_BPF; a denied map cannot exercise packet routing.
    let denied = matches!(
        result,
        Err(datagram::Error::Syscall {
            operation: datagram::Operation::MapCreate,
            errno,
        }) if errno.code().get() == libc::EPERM
    );
    if denied {
        eprintln!("skip: BPF map creation denied (EPERM)");
    }
    !denied
}

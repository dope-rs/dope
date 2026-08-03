use std::io::ErrorKind;
use std::path::Path;

use dope_core::io::socket::addr;
use dope_net::Transport;
use dope_net::unix::{Addr, Unix};

#[test]
fn address_validates_once_into_exact_sockaddr_storage() {
    let addr = Addr::from_path(Path::new("/tmp/dope.sock")).unwrap();
    let raw = Unix::to_sock_addr(&addr).unwrap();

    assert!(raw.socklen() > 0);
    assert_eq!(size_of::<Addr>(), size_of::<addr::Addr>());
    assert_eq!(align_of::<Addr>(), align_of::<addr::Addr>());
    assert_eq!(
        Addr::from_path(Path::new("")).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );

    let oversized = "x".repeat(256);
    assert_eq!(
        Addr::from_path(Path::new(&oversized)).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

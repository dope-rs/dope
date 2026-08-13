use std::{io::ErrorKind, path::Path};

use dope_core::io::socket;
use dope_net::{
    Transport,
    unix::{Addr, Unix},
};

#[test]
fn address_validates_once_into_exact_sockaddr_storage() {
    let addr = Addr::from_path(Path::new("/tmp/dope.sock")).unwrap();
    Unix::to_sock_addr(&addr).unwrap();

    assert_eq!(size_of::<Addr>(), size_of::<socket::Addr>());
    assert_eq!(align_of::<Addr>(), align_of::<socket::Addr>());
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

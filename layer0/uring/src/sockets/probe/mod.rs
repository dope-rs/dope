use std::{net, os::fd};

use dope_sys::{
    Errno,
    io::socket::{self, Socket as _, addr},
};
pub(crate) use loopback::Loopback;
pub(crate) use pair::Pair;

use crate::Uring;

mod loopback;
mod pair;

fn bind(kind: libc::c_int) -> Result<fd::OwnedFd, Errno> {
    let address = addr::Internet::from_std::<Uring>(
        net::SocketAddrV4::new(net::Ipv4Addr::LOCALHOST, 0).into(),
    );
    let file = Uring::socket(socket::Domain::Inet4.raw(), kind)?;
    address
        .as_addr()
        .try_bind::<Uring, Errno>(fd::AsFd::as_fd(&file))?;
    Ok(file)
}

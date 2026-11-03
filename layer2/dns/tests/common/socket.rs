use std::{
    net::{Ipv4Addr, SocketAddr},
    os::fd::{AsFd as _, OwnedFd},
};

use dope::core::{
    Backend, Errno,
    dope_sys::io::socket::{self, Prepare as _, Socket as _, addr},
};
use dope_test::harness::peer::Error;

pub(super) fn bind() -> Result<(SocketAddr, OwnedFd, OwnedFd), Error> {
    for attempt in 0..32 {
        let tcp = Backend::into_fd(
            Backend::open(socket::Domain::Inet4, socket::Kind::Stream).map_err(Error::Create)?,
        );
        let address = addr::Internet::from_std::<Backend>((Ipv4Addr::LOCALHOST, 0).into());
        address.as_addr().try_bind::<Backend, Errno>(tcp.as_fd())?;
        Backend::listen::<Errno>(tcp.as_fd(), socket::BACKLOG)?;
        let mut address = addr::Storage::empty();
        addr::Addr::local::<Backend, Errno>(tcp.as_fd(), &mut address).map_err(Error::Local)?;
        let address = address.to_std().map_err(Error::Address)?;
        let udp = Backend::into_fd(
            Backend::open(socket::Domain::Inet4, socket::Kind::Dgram).map_err(Error::Create)?,
        );
        match addr::Internet::from_std::<Backend>(address)
            .as_addr()
            .try_bind::<Backend, Errno>(udp.as_fd())
        {
            Ok(()) => return Ok((address, tcp, udp)),
            Err(error) if error.code().get() == libc::EADDRINUSE && attempt != 31 => {}
            Err(error) => return Err(Error::Syscall(error)),
        }
    }
    unreachable!("attempts")
}

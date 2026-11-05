use std::{net, os::fd::AsFd as _, time};

use dope_core::{
    Backend, Errno,
    dope_sys::io::{
        poll,
        socket::{self, Prepare as _, addr},
    },
};
use dope_runtime::process::lifecycle::Control;

use crate::harness::{
    GUARD,
    peer::{Error, Stream, scripts},
};

pub(super) fn open<'a>(
    address: net::SocketAddr,
    control: Option<Control<'a>>,
) -> Result<Stream<'a>, Error> {
    scripts::check(control)?;
    let address = addr::Internet::from_std::<Backend>(address);
    let socket = Backend::open(address.domain(), socket::Kind::Stream).map_err(Error::Create)?;
    let stream = Stream::new(Backend::into_fd(socket), control)?;
    match address
        .as_addr()
        .try_connect::<Backend, Errno>(stream.as_fd())
    {
        Ok(()) => {}
        Err(error) if error == Errno::IN_PROGRESS => {
            let ready = scripts::ready(
                control,
                poll::Entry::writable(stream.as_fd()),
                time::Instant::now(),
                Some(GUARD),
            )?
            .ok_or(Error::TimedOut)?;
            raw::finish(stream.as_fd())?;
            if ready & (libc::POLLHUP | libc::POLLERR) != 0 {
                return Err(Error::ConnectEvents(ready));
            }
        }
        Err(error) => return Err(Error::Syscall(error)),
    }
    Ok(stream)
}

mod raw {
    use std::os::fd;

    use dope_core::{
        Backend, Errno,
        dope_sys::{io::socket::Socket as _, kernel},
    };

    pub(super) fn finish(file: fd::BorrowedFd<'_>) -> Result<(), Errno> {
        let error = Backend::get_option::<{ libc::SO_ERROR }, Errno>(file)?;
        // SAFETY: SO_ERROR returns exactly zero or a positive native error code.
        match unsafe { kernel::raw::Code::from_native(error as libc::intptr_t) } {
            Some(error) => Err(error.into()),
            None => Ok(()),
        }
    }
}

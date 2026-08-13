use std::{io, net};

use crate::{
    backend::{self, fixed, kqueue::descriptor},
    driver::{self, ops},
    io::{fd::handles, socket},
};

impl<'d> ops::Bootstrap<'d> for driver::Context<'_, 'd> {
    fn bind_listener_slot(
        &mut self,
        addr: net::SocketAddr,
        backlog: i32,
        config: &socket::ListenerConfig,
    ) -> io::Result<(handles::Descriptor<'d>, net::SocketAddr)> {
        if config.fast_open_backlog().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "TCP Fast Open listener mode requires Linux",
            ));
        }
        let reference = self.driver_ref();
        let handle =
            descriptor::Handle::open(socket::Domain::for_addr(&addr), socket::Kind::Stream)?;
        handle.apply_reuse(config)?;
        handle.bind(&socket::Addr::from_std(addr))?;
        handle.listen(backlog)?;
        let actual = handle.local_addr()?;
        let slot = register(self.backend(), handle, reference)?;
        let descriptor = handles::Descriptor::from_reserved_slot(slot, reference)
            .ok_or_else(|| io::Error::other("dope: fixed ready slot is retired"))?;
        Ok((descriptor, actual))
    }

    fn bind_datagram_slot_raw(
        &mut self,
        addr: net::SocketAddr,
    ) -> io::Result<(handles::Descriptor<'d>, net::SocketAddr)> {
        let reference = self.driver_ref();
        let handle =
            descriptor::Handle::open(socket::Domain::for_addr(&addr), socket::Kind::Dgram)?;
        handle.apply_reuse(&socket::ListenerConfig::for_datagram(&addr))?;
        handle.bind(&socket::Addr::from_std(addr))?;
        let actual = handle.local_addr()?;
        let slot = register(self.backend(), handle, reference)?;
        let descriptor = handles::Descriptor::from_reserved_slot(slot, reference)
            .ok_or_else(|| io::Error::other("dope: fixed ready slot is retired"))?;
        Ok((descriptor, actual))
    }
}

fn register<'d>(
    backend: &mut backend::Kqueue,
    handle: descriptor::Handle,
    driver: driver::Reference<'d>,
) -> io::Result<fixed::Slot<'d>> {
    let slot = backend.files.alloc(driver)?;
    backend.files.install_outbound(slot.fixed(), handle);
    Ok(slot)
}

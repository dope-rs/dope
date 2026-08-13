use std::{io, net};

use dope_core::{
    driver::{self, lifecycle, lifecycle::routing, ops, route},
    io::fd::handles,
};

pub(super) struct Binding<'d, const ID: u8> {
    route: routing::Route<'d, ID>,
    fixed_fd: handles::DatagramDescriptor<'d>,
    bound_addr: net::SocketAddr,
}

impl<'d, const ID: u8> Binding<'d, ID> {
    pub(super) fn bind(
        addr: net::SocketAddr,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<(Self, route::Target<'d, super::RecvTag<ID>>)> {
        let mut route = routing::Route::reserve_transaction(driver)?;
        let (fixed_fd, bound_addr) = ops::Bootstrap::bind_datagram_slot(route.driver(), addr)?;
        let target = route::Space::for_driver(fixed_fd.driver())
            .bind(super::RECV_ARM_TAG, route::Epoch::INITIAL);
        let route = route.commit();
        Ok((
            Self {
                route,
                fixed_fd,
                bound_addr,
            },
            target,
        ))
    }

    pub(super) fn descriptor(&self) -> &handles::DatagramDescriptor<'d> {
        &self.fixed_fd
    }

    pub(super) fn install(&self, install: &mut lifecycle::Install<'_, 'd>) {
        self.route.install(install);
    }

    pub(super) fn local_addr(&self) -> net::SocketAddr {
        self.bound_addr
    }

    pub(super) fn finish(&self, context: &mut lifecycle::Finalize<'_, 'd>) {
        context.retire_route(&self.route);
    }

    pub(super) fn assert_droppable(&self) {
        self.route.assert_droppable();
    }
}

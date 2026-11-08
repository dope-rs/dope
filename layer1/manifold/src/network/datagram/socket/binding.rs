use std::net;

use dope_core::{
    driver::{
        self,
        route::{self, Identity, kind::Recv, lease},
    },
    io::{
        datagram::Lease,
        fd::handles,
        socket::{
            self,
            install::{self, Install},
        },
    },
};
use dope_net;

use crate::{app::creation, network::datagram};

pub(in crate::network::datagram) struct Binding<'d, ID: Identity> {
    fixed_fd: lease::Owner<'d, ID, handles::Datagram<'d>>,
    bound_addr: net::SocketAddr,
}

type Installed<'d, ID> = (
    Binding<'d, ID>,
    route::Target<'d, <ID as route::Materialize<Recv>>::With>,
    Option<route::Epoch>,
);

impl<'d, ID: Identity> Binding<'d, ID> {
    pub(in crate::network::datagram) fn bind(
        addr: net::SocketAddr,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<Installed<'d, ID>, creation::Error> {
        let config =
            socket::Config::for_datagram((addr.port() != 0).then_some(socket::Reuse::Port));
        Self::install(driver, |driver| {
            Install::bind_udp(driver.reborrow(), addr, config)
        })
    }

    pub(in crate::network::datagram) fn adopt<'g: 'd>(
        binding: Lease<'g>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<Installed<'d, ID>, creation::Error> {
        Self::install(driver, |driver| {
            Install::adopt_routed(driver.reborrow(), binding)
        })
    }

    fn install(
        driver: &mut driver::Context<'_, 'd>,
        install: impl FnOnce(
            &mut driver::Context<'_, 'd>,
        )
            -> Result<(handles::Datagram<'d>, net::SocketAddr), install::Error>,
    ) -> Result<Installed<'d, ID>, creation::Error> {
        let mut route = lease::Owner::try_reserve(driver).map_err(creation::Error::Route)?;
        let bound = install(route.driver());
        let (fixed_fd, addr) = bound
            .map_err(dope_net::BindingError::Bootstrap)
            .map_err(creation::Error::Binding)?;
        let target = fixed_fd
            .driver()
            .targets()
            .bind(datagram::RECV_ARM_TAG, route::Epoch::INITIAL);
        let claim = route.into_claim();
        let floor = claim.next_generation();
        let fixed_fd = claim.store(fixed_fd);
        Ok((
            Self {
                fixed_fd,
                bound_addr: addr,
            },
            target,
            floor,
        ))
    }

    pub(in crate::network::datagram) fn advance_to(&self, floor: Option<route::Epoch>) {
        self.fixed_fd.advance_to(floor);
    }

    pub(in crate::network::datagram) fn descriptor(&self) -> &handles::Datagram<'d> {
        &self.fixed_fd
    }

    pub(in crate::network::datagram) fn local_addr(&self) -> net::SocketAddr {
        self.bound_addr
    }
}

const _: () = assert!(
    size_of::<lease::Owner<'static, route::KeyTag<0>, handles::Datagram<'static>>>()
        == size_of::<(
            lease::Owner<'static, route::KeyTag<0>>,
            handles::Datagram<'static>
        )>()
);

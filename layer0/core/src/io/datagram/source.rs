use std::net;

use dope_sys::io::socket::{
    gso,
    msg::source::{self, Source as _},
};

use crate::{
    Backend,
    io::{
        datagram::{GsoControl, GsoSegment},
        socket::msg,
    },
};

/// A destination and its once-selected source, retained for every datagram.
#[derive(Clone, Copy)]
pub struct Source {
    local: Local,
    peer: net::SocketAddr,
}

#[derive(Clone, Copy)]
struct Local {
    address: net::SocketAddr,
    control: Control,
}

/// Immutable native ancillary bytes copied into the submitted message owner.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Control(<Backend as source::Source>::Control);

/// Source selection and optional segmentation retained by one submitted owner.
#[repr(transparent)]
pub struct Segmented(<Backend as source::Source>::Combined);

impl Source {
    /// Selects the source IP and scope without occupying the endpoint's port.
    pub fn try_new(bind: net::SocketAddr, peer: net::SocketAddr) -> Result<Self, source::Error> {
        let (local, control) = Backend::source(bind, peer)?;
        Ok(Self {
            local: Local {
                address: local.get(),
                control: Control(control),
            },
            peer,
        })
    }

    /// The selected IP and scope, retaining the port supplied at construction.
    pub const fn local(&self) -> net::SocketAddr {
        self.local.address
    }

    pub const fn peer(&self) -> net::SocketAddr {
        self.peer
    }

    pub const fn control(&self) -> Control {
        self.local.control
    }
}

impl<M: gso::Mode> Segmented
where
    Backend: gso::Source<Mode = M>,
{
    pub fn new(control: Control, segment: Option<GsoSegment>) -> Self {
        Self(Backend::compose(
            control.0,
            segment.map(|segment| GsoControl::new(segment).0),
        ))
    }

    pub fn into_parts(self) -> (Control, Option<GsoSegment>) {
        let (control, gso) = Backend::release(self.0);
        (
            Control(control),
            gso.map(|gso| GsoControl(gso).into_segment()),
        )
    }

    pub fn attach<'a>(&'a mut self, message: &mut msg::Builder<'a>) {
        message.control(Backend::bytes(&mut self.0));
    }
}

impl AsRef<[u8]> for Control {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

const _: () =
    assert!(size_of::<Source>() == size_of::<(net::SocketAddr, net::SocketAddr, Control)>());

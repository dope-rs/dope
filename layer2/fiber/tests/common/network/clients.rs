use std::net::SocketAddr;

use dope::{
    core::io::{
        socket,
        socket::{Inet, option::Set},
    },
    net,
    net::wire::Identity,
};
use dope_core::driver::route::KeyTag;
use dope_fiber::net::client;

pub struct Addr;

pub struct Transport;

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
pub struct App<'p, 'scope, 'd> {
    #[pin]
    #[manifold]
    pub(crate) conn: client::Machine<'p, 'scope, 'd, 'd, KeyTag<0>, Transport, Identity>,
}

impl net::Transport for Transport {
    type Addr = Addr;
    type Prepared = socket::Inet;
    type StreamConfig = ();

    fn options(_: Self::StreamConfig) -> Set {
        Set::default()
    }
}

impl From<&Addr> for socket::Inet {
    fn from(_: &Addr) -> Self {
        Inet::from_std(SocketAddr::from(([127, 0, 0, 1], 0)))
    }
}

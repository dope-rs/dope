use std::{cell::Cell, net::SocketAddr, rc::Rc};

use dope_manifold::client::attempt::StreamTarget;
use dope_net::tcp;
pub struct Loopback(pub(crate) u16);
impl Loopback {
    pub fn addr(self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.0))
    }
    pub fn target(self) -> StreamTarget<tcp::Tcp> {
        StreamTarget::new(self.addr(), Default::default())
    }
}
pub trait Counted {
    fn counted<'a>(&self) -> Dropped<Box<dyn FnOnce() + 'a>>;
}
impl Counted for Rc<Cell<usize>> {
    fn counted<'a>(&self) -> Dropped<Box<dyn FnOnce() + 'a>> {
        let drops = self.clone();
        Dropped::new(Box::new(move || drops.set(drops.get() + 1)))
    }
}

/// Runs lifecycle observation after the observed owner begins destruction.
pub struct Dropped<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> Dropped<F> {
    pub fn new(callback: F) -> Self {
        Self(Some(callback))
    }

    pub fn empty() -> Self {
        Self(None)
    }
}

impl<F: FnOnce()> Drop for Dropped<F> {
    fn drop(&mut self) {
        if let Some(callback) = self.0.take() {
            callback();
        }
    }
}

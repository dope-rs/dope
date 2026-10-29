use dope_sys::io::socket::msg::{Header, Iovec};
use o3::buffer::bytes::bounded;

use crate::io::{socket, socket::msg};

/// Configures one send header while retaining every installed resource.
///
pub struct Builder<'a> {
    pub(super) header: &'a mut Header,
}

impl<'a> Builder<'a> {
    pub fn new(header: &'a mut Header) -> Self {
        *header = Header::new();
        Self { header }
    }

    pub fn name(&mut self, addr: &'a socket::Inet) {
        use socket::Prepared as _;

        self.header.name(addr.view().native());
    }

    /// Installs ancillary bytes and retains their borrow with the graph.
    pub fn control(&mut self, control: &'a [u8]) {
        self.header.control(control);
    }

    #[must_use]
    pub fn finish<T: AsRef<[u8]>>(
        self,
        iovecs: &'a mut [Iovec; 1],
        payload: &'a bounded::Value<T>,
    ) -> msg::Message<'a> {
        *iovecs = [Iovec::from_slice(payload.as_ref())];
        self.header.bind(iovecs);
        msg::Message {
            header: self.header,
        }
    }
}

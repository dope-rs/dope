use std::io;

use dope::driver::token::Token;
use o3::buffer::Shared;

use crate::Fiber;
use crate::net::port::Port;
use crate::raw::streams::{Read, WriteAll, WriteAllShared};

pub struct Io<'scope, 'd> {
    port: &'scope Port<'d>,
    id: Token,
}

impl<'scope, 'd> Io<'scope, 'd> {
    pub(crate) fn new(port: &'scope Port<'d>, id: Token) -> Self {
        Self { port, id }
    }

    pub(crate) fn handle(&self) -> (&Port<'d>, Token) {
        (self.port, self.id)
    }

    pub fn write_all<'a>(
        &'a mut self,
        data: &'a [u8],
    ) -> impl Fiber<'d, Output = io::Result<()>> + 'a {
        WriteAll::new(self, data)
    }

    pub fn write_all_shared(
        &mut self,
        bytes: Shared,
    ) -> impl Fiber<'d, Output = io::Result<()>> + '_ {
        WriteAllShared::new(self, bytes)
    }

    pub fn read(
        &mut self,
        buf: Vec<u8>,
    ) -> impl Fiber<'d, Output = (io::Result<usize>, Vec<u8>)> + '_ {
        Read::new(self, buf)
    }
}

impl Drop for Io<'_, '_> {
    fn drop(&mut self) {
        self.port.close(self.id);
    }
}

use std::{
    io::{self, Write as _},
    net,
};

use crate::harness::peer;

#[derive(Debug)]
pub enum Half {
    Write(io::Error),
    Shutdown(io::Error),
    Read(io::Error),
}

impl Half {
    pub fn try_run(mut stream: &peer::Stream<'_>, request: &[u8]) -> Result<Vec<u8>, Self> {
        stream.write_all(request).map_err(Self::Write)?;
        stream
            .shutdown(net::Shutdown::Write)
            .map_err(Self::Shutdown)?;
        peer::Peer::try_read_all(stream).map_err(Self::Read)
    }
}

use std::net;

use crate::harness::peer;

#[derive(Debug)]
pub enum Half {
    Write(peer::Error),
    Shutdown(peer::Error),
    Read(peer::Error),
}

impl Half {
    pub fn try_run(stream: &peer::Stream<'_>, request: &[u8]) -> Result<Vec<u8>, Self> {
        stream.write_all(request).map_err(Self::Write)?;
        stream
            .shutdown(net::Shutdown::Write)
            .map_err(Self::Shutdown)?;
        peer::Peer::try_read_all(stream).map_err(Self::Read)
    }
}

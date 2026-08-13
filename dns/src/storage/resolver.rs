use std::io;

use crate::{
    discovery,
    transport::{datagram, stream},
};

/// Resolver proof issued only after both DNS transports bind.
pub struct Resolver<
    'd,
    const DATAGRAM_ID: u8,
    const STREAM_ID: u8,
    const LANES: usize,
    const ADDRESSES: usize,
> {
    storage: &'d crate::Storage<LANES, ADDRESSES>,
    datagram: datagram::Datagram<'d, DATAGRAM_ID, LANES, ADDRESSES>,
    stream: stream::Stream<'d, STREAM_ID, LANES, ADDRESSES>,
}

impl<'d, const DATAGRAM_ID: u8, const STREAM_ID: u8, const LANES: usize, const ADDRESSES: usize>
    Resolver<'d, DATAGRAM_ID, STREAM_ID, LANES, ADDRESSES>
{
    pub(crate) const fn new(
        storage: &'d crate::Storage<LANES, ADDRESSES>,
        datagram: datagram::Datagram<'d, DATAGRAM_ID, LANES, ADDRESSES>,
        stream: stream::Stream<'d, STREAM_ID, LANES, ADDRESSES>,
    ) -> Self {
        Self {
            storage,
            datagram,
            stream,
        }
    }

    pub fn discovery(
        &self,
        target: crate::Target,
    ) -> io::Result<discovery::Discovery<'d, LANES, ADDRESSES>> {
        self.storage.discovery_bound(target)
    }

    pub fn into_parts(
        self,
    ) -> (
        datagram::Datagram<'d, DATAGRAM_ID, LANES, ADDRESSES>,
        stream::Stream<'d, STREAM_ID, LANES, ADDRESSES>,
    ) {
        (self.datagram, self.stream)
    }
}

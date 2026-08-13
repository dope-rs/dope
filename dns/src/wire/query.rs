use std::io;

use dope::net::link::egress::data;
use o3::buffer::{pool, write};

use crate::wire;

const LEGACY_DATAGRAM_BYTES: usize = 512;
const QUESTION_TRAILER_BYTES: usize = 4;
const OPT_RECORD_BYTES: usize = 11;
const QUERY_OVERHEAD_BYTES: usize = wire::HEADER_LEN + QUESTION_TRAILER_BYTES + OPT_RECORD_BYTES;
pub(crate) const MAX_QUERY_BYTES: usize = QUERY_OVERHEAD_BYTES + crate::Name::MAX_WIRE_LEN;
const STREAM_PREFIX_BYTES: usize = 2;
pub(crate) const MAX_STREAM_FRAME_BYTES: usize = STREAM_PREFIX_BYTES + MAX_QUERY_BYTES;

const _: () = assert!(MAX_QUERY_BYTES <= LEGACY_DATAGRAM_BYTES);
const _: () = assert!(MAX_STREAM_FRAME_BYTES <= u16::MAX as usize);

pub(crate) struct Query<'a> {
    id: wire::TransactionId,
    name: &'a crate::Name,
    record_type: u16,
}

impl<'a> Query<'a> {
    pub(crate) const fn new(
        id: wire::TransactionId,
        name: &'a crate::Name,
        record_type: u16,
    ) -> Self {
        Self {
            id,
            name,
            record_type,
        }
    }

    pub(crate) fn encode_datagram(self, output: &mut pool::Cursor) -> io::Result<()> {
        output.truncate(0);
        if output.spare_capacity() < MAX_QUERY_BYTES {
            return Err(Self::invalid(
                "DNS query buffer is smaller than the fixed wire bound",
            ));
        }
        let expected = self.encoded_len();
        self.encode(output)?;
        debug_assert_eq!(output.len(), expected);
        Ok(())
    }

    pub(crate) fn encode_stream(self) -> io::Result<data::Inline<MAX_STREAM_FRAME_BYTES>> {
        let expected = self.encoded_len();
        let length = u16::try_from(expected)
            .map_err(|_| Self::invalid("DNS query exceeds the TCP length prefix"))?;
        let mut frame = data::Inline::new();
        frame
            .try_extend(&length.to_be_bytes())
            .map_err(|_| Self::invalid("DNS TCP prefix exceeds the fixed frame"))?;
        self.encode(&mut frame)?;
        debug_assert_eq!(frame.len(), STREAM_PREFIX_BYTES + expected);
        Ok(frame)
    }

    const fn encoded_len(&self) -> usize {
        QUERY_OVERHEAD_BYTES + self.name.wire_len()
    }

    fn encode(self, output: &mut impl write::ByteSink) -> io::Result<()> {
        Self::push_u16(output, self.id.wire())?;
        Self::push_u16(output, 0x0100)?;
        Self::push_u16(output, 1)?;
        Self::push_u16(output, 0)?;
        Self::push_u16(output, 0)?;
        Self::push_u16(output, 1)?;
        Self::encode_name(output, self.name)?;
        Self::push_u16(output, self.record_type)?;
        Self::push_u16(output, wire::IN)?;
        Self::push(output, 0)?;
        Self::push_u16(output, wire::OPT)?;
        Self::push_u16(output, wire::EDNS_PAYLOAD)?;
        Self::push_u32(output, 0)?;
        Self::push_u16(output, 0)
    }

    fn encode_name(output: &mut impl write::ByteSink, name: &crate::Name) -> io::Result<()> {
        for label in name.labels() {
            Self::push(output, label.len() as u8)?;
            Self::extend(output, label)?;
        }
        Self::push(output, 0)
    }

    fn push(output: &mut impl write::ByteSink, value: u8) -> io::Result<()> {
        output
            .write_byte(value)
            .map_err(|_| Self::invalid("DNS query exceeds its fixed buffer"))
    }

    fn extend(output: &mut impl write::ByteSink, bytes: &[u8]) -> io::Result<()> {
        output
            .write_slice(bytes)
            .map_err(|_| Self::invalid("DNS query exceeds its fixed buffer"))
    }

    fn push_u16(output: &mut impl write::ByteSink, value: u16) -> io::Result<()> {
        Self::extend(output, &value.to_be_bytes())
    }

    fn push_u32(output: &mut impl write::ByteSink, value: u32) -> io::Result<()> {
        Self::extend(output, &value.to_be_bytes())
    }

    fn invalid(message: &'static str) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, message)
    }
}

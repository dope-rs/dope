use crate::wire::packet::{self, encode, header, kind, long};
use header::widths::Width;
use std::num::NonZero;

/// An encoded header retaining the exact body used to determine its wire length.
pub(crate) struct Body<'header, 'body, K: kind::Kind> {
    header: header::Ref<'header, K>,
    body: &'body [u8],
}

impl<'header, 'body, K: kind::Kind> Body<'header, 'body, K> {
    /// Long payloads fit VarInt; short headers are bounded by the CID and PN widths.
    pub(crate) fn wire_len(&self) -> NonZero<usize> {
        let width = Width::from_pn(self.header.layout.encoding());
        let body = header::Payload::new(self.body.len(), width).get();
        NonZero::<usize>::MIN.saturating_add(self.header.bytes.len() + body + packet::TAG_LEN - 1)
    }

    pub(crate) fn into_parts(self) -> (&'header [u8], header::Layout<K>, &'body [u8]) {
        (self.header.bytes, self.header.layout, self.body)
    }
}

impl<'header, 'body, K: kind::Long> Body<'header, 'body, K> {
    pub(crate) fn try_new(
        fields: long::Fields<'_>,
        out: &'header mut Vec<u8>,
        body: &'body [u8],
    ) -> Result<Self, encode::Error> {
        let header = fields.try_header(out, body.len())?;
        Ok(Self { header, body })
    }
}

const _: () = {
    assert!(size_of::<Body<'static, 'static, kind::Initial>>() == 6 * size_of::<usize>());
};

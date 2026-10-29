use o3::buffer::bytes::bounded::Value;

use crate::io::datagram::{GsoPayload, GsoSegment};

/// One admitted GSO owner at its final native lowering boundary.
#[repr(transparent)]
pub struct Payload<P>(GsoPayload<P>);

impl<P> From<GsoPayload<P>> for Payload<P> {
    fn from(payload: GsoPayload<P>) -> Self {
        Self(payload)
    }
}

impl<P> Payload<P> {
    /// # Safety
    /// Preserve the exact payload bytes and segment pairing through submission
    /// and retries; owner conversion must preserve the byte range and length.
    pub unsafe fn into_parts(self) -> (Value<P>, GsoSegment) {
        (self.0.payload, self.0.segment)
    }
}

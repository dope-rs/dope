use crate::{
    lookup::error,
    wire::packet::{self, Labels, Offset, Packet, Part},
};

/// A complete name traversal, validated once against its retained packet.
pub(crate) struct Expanded<'packet> {
    packet: Packet<'packet>,
    position: Offset,
    remaining: usize,
}

impl<'packet> Expanded<'packet> {
    pub(super) fn try_new(labels: Labels<'packet>) -> Result<Self, error::Packet> {
        let mut scan = labels.clone();
        while scan.next().transpose()?.is_some() {}
        Ok(Self {
            packet: labels.packet,
            position: labels.position,
            remaining: scan.expanded_len,
        })
    }

    pub(crate) fn into_parts(self) -> (Option<usize>, impl Iterator<Item = &'packet [u8]>) {
        (self.remaining.checked_sub(1), self)
    }
}

impl<'packet> Iterator for Expanded<'packet> {
    type Item = &'packet [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        loop {
            // SAFETY: construction checked the entire immutable traversal.
            // Positive remaining length proves a nonempty label precedes the
            // terminator; every range, pointer and tag retains that proof.
            match unsafe {
                (packet::raw::Label {
                    packet: self.packet,
                    position: self.position,
                })
                .read()
            } {
                Part::Pointer { target, .. } => self.position = target,
                Part::Label(label) => {
                    self.position = label.end;
                    self.remaining -= label.bytes.len() + 1;
                    return Some(label.bytes);
                }
            }
        }
    }
}

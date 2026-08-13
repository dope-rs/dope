use crate::link::egress::data;

/// Contiguous, transactional output used by protocol encoders.
/// Implementations record overflow instead of partially committing a frame.
/// Generic encoders remain statically dispatched across pooled cursors.
pub trait Write {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn overflowed(&self) -> bool;

    fn push(&mut self, byte: u8);

    fn extend_from_slice(&mut self, bytes: &[u8]);

    fn as_mut_slice(&mut self) -> &mut [u8];
}

impl Write for data::Cursor {
    fn len(&self) -> usize {
        self.len()
    }

    fn overflowed(&self) -> bool {
        self.overflowed()
    }

    fn push(&mut self, byte: u8) {
        let _ = self.try_push(byte);
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        let _ = self.try_extend(bytes);
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

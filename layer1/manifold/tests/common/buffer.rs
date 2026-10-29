use dope_core::io::transfer;
use dope_net::wire::send;
use o3::{base::marker::ThreadBound, buffer::storage::inline};

#[derive(Default)]
pub struct Buffer<const CAP: usize> {
    pub(crate) bytes: inline::Bytes<CAP, u16>,
    pub(crate) _thread: ThreadBound,
}

impl<const CAP: usize> send::Store for Buffer<CAP> {
    fn as_slice(&self) -> &[u8] {
        self.bytes.as_ref()
    }

    fn release(self) -> send::Availability {
        send::Availability::Unchanged
    }
}

impl<const CAP: usize> send::Consume for Buffer<CAP> {
    fn consume_max(&mut self, max: transfer::Len) {
        let amount = transfer::Len::prefix(self.bytes.len(), max);
        let used = amount.into_raw();
        let len = self.bytes.len();
        self.bytes.backing_mut().copy_within(used..len, 0);
        self.bytes.truncate(len - used);
    }
}

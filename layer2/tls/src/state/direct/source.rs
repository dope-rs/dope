use crate::state::direct;
use dope::core::io::recv;
use o3::buffer::{self, bytes::bounded, write::consume};

/// Retains the authenticated end while the remaining suffix only shrinks.
pub(crate) struct Cursor<'d> {
    bytes: recv::Shared<'d>,
    end: usize,
    len: usize,
}

impl<'d> Cursor<'d> {
    pub(crate) fn new(plain: direct::Plain<recv::Lease<'d>>) -> Self {
        Self {
            bytes: plain.bytes.into_shared(),
            end: plain.len,
            len: plain.len,
        }
    }

    pub(crate) fn view(&self) -> <recv::Shared<'d> as buffer::Source>::View<'_> {
        let start = self.end - self.len;
        let original = buffer::Complete::view(&self.bytes).into_inner();
        // SAFETY: Input::owned initializes this prefix in this lease. Consumption only narrows it.
        let proof = unsafe { bounded::raw::Input::new(original).select(start..self.end) };
        bounded::Value::from_raw(proof)
    }
}

impl AsRef<[u8]> for Cursor<'_> {
    fn as_ref(&self) -> &[u8] {
        buffer::Read::as_slice(&self.view())
    }
}

impl consume::Consume for Cursor<'_> {
    fn remaining(&self) -> usize {
        self.len
    }

    fn try_consume(&mut self, amount: usize) -> Result<(), consume::Error> {
        consume::Error::check(amount, self.len)?;
        self.len -= amount;
        Ok(())
    }

    fn consume_max(&mut self, max: usize) -> usize {
        let amount = max.min(self.len);
        self.len -= amount;
        amount
    }
}

// SAFETY: the branded end belongs to this cursor's current authenticated prefix.
unsafe impl buffer::raw::Consume for Cursor<'_> {
    fn consume<const MAX: u32>(prefix: buffer::raw::Consumed<'_, '_, Self, MAX>) {
        let (cursor, end) = prefix.into_parts();
        cursor.len -= end.count().get() as usize;
    }
}

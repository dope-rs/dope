use super::Driver;
use crate::Lend;

impl Lend for Driver {
    fn group(&self) -> u16 {
        // SAFETY: leaf read.
        unsafe { self.inner() }.provided.group()
    }

    fn release(&self, bid: Option<u16>) {
        // SAFETY: leaf.
        let this = unsafe { self.inner() };
        if let Some(b) = bid {
            crate::memstats::provided_release();
            this.provided.defer(b);
        }
    }

    unsafe fn slice<'a>(&self, len: u32, bid: u16) -> &'a [u8] {
        crate::memstats::provided_borrow();
        // SAFETY: leaf read; caller guarantees bid is valid + held until release.
        unsafe { self.inner().provided.slice(bid, len as usize) }
    }
}

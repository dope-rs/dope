use super::Driver;
use crate::Lend;

impl Lend for Driver {
    fn group(&self) -> u16 {
        self.provided.group()
    }

    fn release(&mut self, bid: Option<u16>) {
        if let Some(b) = bid {
            crate::memstats::provided_release();
            self.provided.defer(b);
        }
    }

    unsafe fn slice<'a>(&self, len: u32, bid: u16) -> &'a [u8] {
        crate::memstats::provided_borrow();
        // SAFETY: caller guarantees bid is valid + held until release; len is byte count.
        unsafe { self.provided.slice(bid, len as usize) }
    }
}

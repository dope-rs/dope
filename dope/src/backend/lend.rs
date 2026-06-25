pub trait Lend {
    fn group(&self) -> u16;

    fn release(&mut self, bid: Option<u16>);

    /// # Safety
    /// The caller guarantees `bid` names a filled buffer of `len` bytes owned by
    /// the driver and held valid for `'a`.
    unsafe fn slice<'a>(&self, len: u32, bid: u16) -> &'a [u8];
}

pub struct BidGuard<'a, D: ?Sized + Lend> {
    driver: &'a mut D,
    bid: u16,
    slice: &'a [u8],
}

impl<'a, D: ?Sized + Lend> BidGuard<'a, D> {
    pub(super) fn new(driver: &'a mut D, len: u32, bid: u16) -> Self {
        // SAFETY: caller guarantees bid valid + held until guard drop; slice lifetime bounded by guard.
        let slice = unsafe { driver.slice(len, bid) };
        Self { driver, bid, slice }
    }

    pub(super) fn slice(&self) -> &'a [u8] {
        self.slice
    }
}

impl<'a, D: ?Sized + Lend> Drop for BidGuard<'a, D> {
    fn drop(&mut self) {
        self.driver.release(Some(self.bid));
    }
}

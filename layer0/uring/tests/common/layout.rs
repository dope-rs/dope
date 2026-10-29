use std::ptr;

use dope_sys::io::file;
use dope_uring::Uring;

type Borrowed<'a> = <Uring as file::File>::Request<'a>;

#[repr(align(8192))]
pub struct Aligned {
    _byte: u8,
}

pub fn encoded(entry: &Borrowed<'_>) -> [u8; 64] {
    // SAFETY: Borrowed transparently wraps Entry's initialized 64-byte ABI,
    // whose size and alignment are asserted in reactor::sq::entry;
    // reactor::sq::borrowed asserts that Borrowed retains the same size.
    unsafe { ptr::from_ref(entry).cast::<[u8; 64]>().read() }
}

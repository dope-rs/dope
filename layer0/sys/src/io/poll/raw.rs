use crate::io::poll::Entry;

/// # Safety
/// The returned slice must never contain more than `MAX` entries.
pub unsafe trait Entries<'fd> {
    const MAX: usize;
    fn entries(&mut self) -> &mut [Entry<'fd>];
}

// SAFETY: coercing this array to a slice retains its exact N-element extent.
unsafe impl<'fd, const N: usize> Entries<'fd> for [Entry<'fd>; N] {
    const MAX: usize = N;
    fn entries(&mut self) -> &mut [Entry<'fd>] {
        self
    }
}

use o3::alloc::{self, BoxSliceExt as _};

pub const PREFIX: usize = 16;

pub fn try_buffer<const NAME: usize, const CONTROL: usize>(
    payload: usize,
) -> Result<Box<[u8]>, alloc::Error> {
    let len = [NAME, CONTROL, payload]
        .into_iter()
        .try_fold(PREFIX, usize::checked_add)
        .ok_or(alloc::Error::Overflow)?;
    Box::<[u8]>::try_repeat(len, 0)
}

pub fn prefix(buffer: &mut [u8; PREFIX], name: u32, control: u32, payload: u32, flags: u32) {
    for (chunk, value) in buffer
        .as_chunks_mut::<4>()
        .0
        .iter_mut()
        .zip([name, control, payload, flags])
    {
        *chunk = value.to_ne_bytes();
    }
}

pub const PREFIX: usize = 16;

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

pub(crate) mod buffer;
pub(crate) mod change;
pub(crate) mod done;
pub(crate) mod permit;
pub(crate) mod registry;
pub(crate) mod table;

pub(crate) fn hash(key: usize) -> u64 {
    let folded = key ^ (key >> 24);
    folded.wrapping_mul(0x9E37_79B9_7F4A_7C15usize) as u64
}

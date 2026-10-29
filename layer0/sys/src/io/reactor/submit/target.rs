/// A direct-file slot whose complete index fits the native 24-bit domain.
pub trait Target: Copy + Eq {
    const ZERO: Self;

    fn from_le(bytes: [u8; 3]) -> Self;
}

/// A logical CPU observed in the calling thread's affinity mask.
///
/// Native discovery produces these values, so carrying a `Cpu` preserves
/// the discovery proof through process layout and thread binding.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Cpu(u16);

impl Cpu {
    /// # Safety
    /// `raw` must have been observed in the current platform's CPU affinity set.
    pub const unsafe fn new(raw: u16) -> Self {
        Self(raw)
    }

    pub const fn raw(self) -> u16 {
        self.0
    }
}

impl From<Cpu> for u64 {
    fn from(cpu: Cpu) -> Self {
        u64::from(cpu.0)
    }
}

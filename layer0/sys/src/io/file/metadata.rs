use crate::io::file::Kind;

/// Native metadata for one opened or named file.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Metadata(libc::stat);

impl Metadata {
    pub(super) const fn new(raw: libc::stat) -> Self {
        Self(raw)
    }

    pub const fn kind(self) -> Kind {
        Kind::from_mode(self.0.st_mode)
    }

    /// Exact signed st_size reported by the native metadata operation.
    pub const fn size(self) -> i64 {
        self.0.st_size
    }
}

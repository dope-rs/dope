/// Native file kind observed from descriptor metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Kind(libc::mode_t);

impl Kind {
    /// Uses the native entry's kind without a second metadata operation.
    /// An absent or unrecognized hint still requires descriptor-relative metadata.
    pub const fn from_dirent(value: u8) -> Option<Self> {
        match value {
            libc::DT_BLK
            | libc::DT_CHR
            | libc::DT_DIR
            | libc::DT_FIFO
            | libc::DT_LNK
            | libc::DT_REG
            | libc::DT_SOCK => Some(Self((value as libc::mode_t) << 12)),
            _ => None,
        }
    }

    pub(super) const fn from_mode(mode: libc::mode_t) -> Self {
        Self(mode & libc::S_IFMT)
    }

    /// Whether this kind is one regular file.
    pub const fn is_regular(self) -> bool {
        self.0 == libc::S_IFREG
    }

    pub const fn is_dir(self) -> bool {
        self.0 == libc::S_IFDIR
    }

    pub const fn is_symlink(self) -> bool {
        self.0 == libc::S_IFLNK
    }
}

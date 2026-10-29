pub mod cleanup;
pub mod layout;
pub mod message;
pub mod options;
pub mod packets;
pub mod receive;
pub mod ring;

pub struct Directory(std::ffi::CString, o3::base::marker::ThreadBound);

impl Directory {
    pub fn try_new(base: &std::path::Path, tag: &str, nonce: u128) -> std::io::Result<Self> {
        use std::os::unix::{ffi::OsStrExt as _, fs::DirBuilderExt as _};

        let path = base.join(format!("du-{tag}-{}-{nonce}", std::process::id()));
        let encoded = std::ffi::CString::new(path.as_os_str().as_bytes())?;
        std::fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(encoded, o3::base::marker::ThreadBound::NEW))
    }

    pub fn path(&self) -> &std::path::Path {
        use std::os::unix::ffi::OsStrExt as _;

        std::path::Path::new(std::ffi::OsStr::from_bytes(self.0.to_bytes()))
    }

    pub fn c_path(&self) -> &std::ffi::CStr {
        &self.0
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir(self.path()).expect("fixture directory cleanup");
    }
}

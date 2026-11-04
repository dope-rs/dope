pub mod cleanup;
pub mod layout;
pub mod message;
pub mod options;
pub mod packets;
pub mod receive;
pub mod ring;

#[derive(Debug)]
pub enum DirectoryError {
    Path(std::ffi::NulError),
    Syscall(dope_sys::Errno),
}

pub struct Directory(std::ffi::CString, o3::base::marker::ThreadBound);

impl Directory {
    pub fn try_new(tag: &str) -> Result<Self, DirectoryError> {
        use dope_sys::io::file::{File as _, Mode};
        use std::os::unix::ffi::OsStrExt as _;

        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("fixture clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("du-{tag}-{}-{nonce}", std::process::id()));
        let encoded =
            std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(DirectoryError::Path)?;
        dope_uring::Uring::mkdir::<dope_sys::Errno>(&encoded, Mode::new(0o700))
            .map_err(DirectoryError::Syscall)?;
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
        use dope_sys::io::file::File as _;

        dope_uring::Uring::remove_dir_at::<dope_sys::Errno>(None, &self.0)
            .expect("fixture directory cleanup");
    }
}

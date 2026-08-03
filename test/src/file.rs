use std::env::temp_dir;
use std::fs::{remove_file, write};
use std::path::Path;
pub struct TempFile(String);

impl TempFile {
    pub fn with(tag: &str, contents: &[u8]) -> Self {
        let path = temp_dir()
            .join(format!("dope_test_{}_{}", std::process::id(), tag))
            .to_string_lossy()
            .into_owned();
        write(&path, contents).expect("write temp");
        Self(path)
    }

    pub fn path(&self) -> &Path {
        Path::new(&self.0)
    }

    pub fn path_str(&self) -> &str {
        &self.0
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = remove_file(&self.0);
    }
}

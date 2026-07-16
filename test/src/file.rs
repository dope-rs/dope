pub struct TempFile(String);

impl TempFile {
    pub fn with(tag: &str, contents: &[u8]) -> Self {
        let path = std::env::temp_dir()
            .join(format!("dope_test_{}_{}", std::process::id(), tag))
            .to_string_lossy()
            .into_owned();
        std::fs::write(&path, contents).expect("write temp");
        Self(path)
    }

    pub fn path(&self) -> &std::path::Path {
        std::path::Path::new(&self.0)
    }

    pub fn path_str(&self) -> &str {
        &self.0
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

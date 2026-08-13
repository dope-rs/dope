use std::{env, fs, io, io::Write as _, path, process, thread, time};

const CREATE_ATTEMPTS: u8 = 64;

fn create_unique<T>(
    tag: &str,
    mut create: impl FnMut(&path::Path) -> io::Result<T>,
) -> io::Result<(path::PathBuf, T)> {
    let nonce = time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_nanos();
    let thread = thread::current().id();
    for attempt in 0..CREATE_ATTEMPTS {
        let path = env::temp_dir().join(format!(
            "dope_test_{}_{thread:?}_{nonce}_{attempt}_{tag}",
            process::id(),
        ));
        match create(&path) {
            Ok(value) => return Ok((path, value)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!("no unique temporary path after {CREATE_ATTEMPTS} attempts"),
    ))
}

fn abort(error: &io::Error) -> ! {
    eprintln!("dope-test temporary resource failure: {error}");
    process::abort()
}

pub struct File(String);

impl File {
    pub fn with(tag: &str, contents: &[u8]) -> Self {
        match Self::try_with(tag, contents) {
            Ok(file) => file,
            Err(error) => abort(&error),
        }
    }

    pub fn try_with(tag: &str, contents: &[u8]) -> io::Result<Self> {
        let (path, mut file) = create_unique(tag, |path| {
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
        })?;
        file.write_all(contents)?;
        Ok(Self(path.to_string_lossy().into_owned()))
    }

    pub fn path(&self) -> &path::Path {
        path::Path::new(&self.0)
    }

    pub fn path_str(&self) -> &str {
        &self.0
    }
}

impl Drop for File {
    fn drop(&mut self) {
        use std::fs::remove_file;
        if let Err(error) = remove_file(&self.0)
            && error.kind() != io::ErrorKind::NotFound
        {
            eprintln!("dope-test temporary file cleanup failure: {error}");
        }
    }
}

pub struct Directory(path::PathBuf);

impl Directory {
    pub fn with(tag: &str) -> Self {
        match Self::try_with(tag) {
            Ok(directory) => directory,
            Err(error) => abort(&error),
        }
    }

    pub fn try_with(tag: &str) -> io::Result<Self> {
        use std::fs::create_dir;

        let (path, ()) = create_unique(tag, |path| create_dir(path))?;
        Ok(Self(path))
    }

    pub fn path(&self) -> &path::Path {
        &self.0
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        use std::fs::remove_dir_all;
        if let Err(error) = remove_dir_all(&self.0)
            && error.kind() != io::ErrorKind::NotFound
        {
            eprintln!("dope-test temporary directory cleanup failure: {error}");
        }
    }
}

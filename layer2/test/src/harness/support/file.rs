use std::{fs, io, os::unix::fs::DirBuilderExt as _, path, process, time};

const CREATE_ATTEMPTS: u8 = 64;

#[derive(Debug)]
pub enum Error {
    Create(io::Error),
    Time(time::SystemTimeError),
    Exhausted,
}

#[must_use = "keep the fixture directory alive until its files are no longer used"]
pub struct Directory<F: FnOnce(io::Result<()>)> {
    path: path::PathBuf,
    report: Option<F>,
}

impl<F: FnOnce(io::Result<()>)> Directory<F> {
    pub fn try_with(base: &path::Path, tag: &str, report: F) -> Result<Self, Error> {
        let nonce = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .map_err(Error::Time)?
            .as_nanos();
        let pid = process::id();
        for attempt in 0..CREATE_ATTEMPTS {
            let path = base.join(format!("dt-{tag}-{pid:x}-{nonce:x}-{attempt:x}"));
            match fs::DirBuilder::new().mode(0o700).create(&path) {
                Ok(()) => {
                    return Ok(Self {
                        path,
                        report: Some(report),
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(Error::Create(error)),
            }
        }
        Err(Error::Exhausted)
    }

    pub fn path(&self) -> &path::Path {
        &self.path
    }

    /// Removes the directory now, returning its result instead of calling the observer.
    pub fn try_close(mut self) -> io::Result<()> {
        let _report = self.report.take();
        self.try_remove()
    }

    /// Removes an owned fixture tree without following entries replaced by symlinks.
    /// The caller retains its own policy for an initially missing root.
    pub fn try_remove(&self) -> io::Result<()> {
        fs::remove_dir_all(&self.path)
    }
}

impl<F: FnOnce(io::Result<()>)> Drop for Directory<F> {
    fn drop(&mut self) {
        if let Some(report) = self.report.take() {
            report(self.try_remove());
        }
    }
}

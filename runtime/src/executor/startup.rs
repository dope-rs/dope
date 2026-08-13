/// Compile-time behavior selected by an executor's shutdown source after its
/// pinned application is installed.
pub trait Startup {
    #[doc(hidden)]
    fn installed(&self);
}

impl Startup for () {
    fn installed(&self) {}
}

impl Startup for crate::shutdown::Source {
    fn installed(&self) {}
}

impl Startup for crate::process::Shutdown {
    fn installed(&self) {
        self.installed();
    }
}

use crate::io;
/// Native completion reports are projected only at the original scope observer.
#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct Report(pub(crate) io::Reclaimed);

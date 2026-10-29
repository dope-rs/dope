use dope_sys::io::request;

/// Descriptor authority carried only by a typed datagram request.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Message<T>(T);

impl<T> Message<T> {
    pub(crate) fn new(target: T) -> Self {
        Self(target)
    }
}

impl<T> request::Datagram for Message<T> {
    type Target = T;

    fn into_target(self) -> T {
        self.0
    }
}

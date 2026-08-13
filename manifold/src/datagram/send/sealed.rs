use dope_core::io::{socket::msg, transfer};

#[repr(transparent)]
pub(in crate::datagram) struct Bounded<T>(T);

impl<T: AsRef<[u8]>> Bounded<T> {
    pub(super) fn try_new(value: T) -> Result<Self, T> {
        if value.as_ref().len() > transfer::MAX_BYTES {
            return Err(value);
        }
        Ok(Self(value))
    }

    pub(super) fn part(&self) -> msg::raw::Part<'_> {
        // SAFETY: construction bounds the length and no mutable access can
        // change the retained value's readable range.
        unsafe { msg::raw::Part::from_bounded(self.0.as_ref()) }
    }
}

impl<T> Bounded<T> {
    pub(super) fn into_inner(self) -> T {
        self.0
    }
}

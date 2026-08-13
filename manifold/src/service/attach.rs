use std::{cell, error, fmt, ops};

/// Irreversible one-shot attachment state for a service-owned resource.
/// Dropping its [`Bound`] does not make the attachment reusable.
pub struct Attach {
    attached: cell::Cell<bool>,
}

impl Attach {
    pub const fn new() -> Self {
        Self {
            attached: cell::Cell::new(false),
        }
    }

    pub fn bind<'d, T: ?Sized>(&self, value: &'d T) -> Result<Bound<'d, T>, AlreadyAttached> {
        if self.attached.replace(true) {
            return Err(AlreadyAttached);
        }
        Ok(Bound { value })
    }
}

impl Default for Attach {
    fn default() -> Self {
        Self::new()
    }
}

/// Affine one-pointer resource selected by [`Attach::bind`].
/// It is intentionally neither [`Copy`] nor [`Clone`].
#[repr(transparent)]
pub struct Bound<'d, T: ?Sized> {
    value: &'d T,
}

impl<'d, T: ?Sized> Bound<'d, T> {
    pub const fn get(&self) -> &'d T {
        self.value
    }
}

impl<T: ?Sized> ops::Deref for Bound<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlreadyAttached;

impl fmt::Display for AlreadyAttached {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("service resource is already attached")
    }
}

impl error::Error for AlreadyAttached {}

const _: () =
    assert!(std::mem::size_of::<Bound<'static, u64>>() == std::mem::size_of::<&'static u64>());

use std::{mem, num};

/// A positive kernel listen depth.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Backlog(num::NonZero<i32>);

impl Backlog {
    /// Creates a listen depth validated during compilation.
    pub const fn fixed<const VALUE: i32>() -> Self {
        const { assert!(VALUE > 0) };
        let value = match num::NonZero::<i32>::new(VALUE) {
            Some(value) => value,
            None => num::NonZero::<i32>::MIN,
        };
        Self(value)
    }

    #[must_use]
    pub(crate) const fn get(self) -> i32 {
        self.0.get()
    }
}

impl From<Backlog> for num::NonZero<i32> {
    fn from(depth: Backlog) -> Self {
        depth.0
    }
}

const _: () = {
    assert!(mem::size_of::<Backlog>() == mem::size_of::<i32>());
    assert!(mem::size_of::<Option<Backlog>>() == mem::size_of::<i32>());
};

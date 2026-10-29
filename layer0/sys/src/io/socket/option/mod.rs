mod cursor;
pub mod fast;
mod step;
pub mod tuning;
mod value;

pub use cursor::Cursor;
pub use step::Step;
pub use value::Value;
mod kind;
mod source;

pub use kind::Kind;
pub use source::Source;

/// One kernel option level, e.g. a protocol number or the socket level.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Level(libc::c_int);

impl Level {
    pub const fn new(raw: i32) -> Self {
        Self(raw)
    }

    pub const fn raw(self) -> i32 {
        self.0
    }
}

/// One kernel option name within its level.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Name(libc::c_int);

impl Name {
    pub const fn new(raw: i32) -> Self {
        Self(raw)
    }

    pub const fn raw(self) -> i32 {
        self.0
    }
}

const _: () = {
    assert!(size_of::<Level>() == size_of::<i32>());
    assert!(size_of::<Name>() == size_of::<i32>());
};

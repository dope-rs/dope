#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Cut {
    Complete,
    Truncated,
}

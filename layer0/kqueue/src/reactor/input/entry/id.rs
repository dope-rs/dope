use crate::reactor::input::entry::Kind;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Id {
    pub(in crate::reactor::input) key: usize,
    pub(in crate::reactor::input) kind: Kind,
}

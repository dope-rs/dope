use crate::{io::reactor::read::Family, table::change};

pub trait Call: Copy {
    type File: change::Id;
    type Key: Copy + Eq;

    fn file(self) -> Self::File;
    fn key(self) -> Self::Key;
    fn family(self) -> Family;
}

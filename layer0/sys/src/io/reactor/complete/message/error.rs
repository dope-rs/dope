use crate::io::reactor::complete::message::Region;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error<C, L> {
    Overflow {
        name: C,
        control: C,
    },
    Short {
        actual: L,
        required: L,
    },
    MissingTruncation {
        region: Region,
        reported: L,
        available: L,
    },
}

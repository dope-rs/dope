#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    LaneCapacity {
        target: crate::Target,
        capacity: usize,
    },
    LiteralName(crate::Target),
}

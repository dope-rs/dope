#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure {
    Slot,
    TargetOccupied,
    TargetFull,
    FileOccupied,
}

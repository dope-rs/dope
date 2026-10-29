#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Poll {
    Pending,
    NewlyReady,
    Staged,
}

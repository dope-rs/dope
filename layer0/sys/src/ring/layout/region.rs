/// The exact native queue mapping addressed by a layout or mapping failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Region {
    Submission,
    Completion,
    Entries,
}

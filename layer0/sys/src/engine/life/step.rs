/// Progress of one affine descriptor retirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Step {
    Empty,
    Advanced,
    Blocked,
}

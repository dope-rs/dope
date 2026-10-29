#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
    Rearm,
    Yield,
    Terminal,
}

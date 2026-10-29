/// Kernel guarantees required before a ring can serve driver requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Feature {
    ExtendedArguments,
    NoDrop,
    SkipCompletionOnSuccess,
}

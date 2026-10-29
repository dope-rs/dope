/// Exact native result rejected by an operation's completion contract.
pub trait Returned: Copy + Eq {
    type Value;
    type Invalid;

    fn value(value: Self::Value) -> Self;
    fn error(code: crate::Errno) -> Self;
    fn invalid(invalid: Self::Invalid) -> Self;
}

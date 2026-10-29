use crate::io::reactor::{
    complete,
    submit::{Poll, Target},
};

/// Native control entries bound to the caller's exact completion tokens.
pub trait Control {
    type Target: Target;
    type Owned<T: complete::raw::Token>;
    type Poll<'a, T: complete::raw::Token>: Poll<'a, T>;

    fn nop<T: complete::raw::Token>(complete: T) -> Self::Owned<T>;
    fn cancel<T: complete::raw::Token>(target: T, complete: T) -> Self::Owned<T>;
    fn close<T: complete::raw::Token>(slot: Self::Target, complete: T) -> Self::Owned<T>;
    /// First step of a hard-linked close, suppressing its successful completion.
    fn shutdown<T: complete::raw::Token>(slot: Self::Target, complete: T) -> Self::Owned<T>;
}

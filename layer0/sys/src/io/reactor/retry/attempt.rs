use crate::Errno;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Attempt<T> {
    Ready(T),
    Blocked(Errno),
}

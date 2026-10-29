use crate::client::attempt::queue::storage::Borrow;

/// The removal authority established by the admitted attempt state.
pub(in crate::client::attempt::queue) enum Access<E> {
    Owned(E),
    Borrowed(Borrow<E>),
}

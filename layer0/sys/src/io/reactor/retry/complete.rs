use crate::io::reactor::{complete, retry::Context};

pub trait Complete<C: Context>: complete::Event<Key = C::Key> {
    fn sent(key: C::Key, result: C::Outcome) -> Self;
    fn connected(key: C::Key, result: C::Unit) -> Self;
}

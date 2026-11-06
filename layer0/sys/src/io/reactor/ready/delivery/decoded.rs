#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decoded<K> {
    Public(K),
    Shutdown(crate::engine::register::Interest),
    Empty,
}

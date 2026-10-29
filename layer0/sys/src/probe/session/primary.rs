/// Exact canary registration stage that failed.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Primary<R, U> {
    Register(R),
    Unregister(U),
}

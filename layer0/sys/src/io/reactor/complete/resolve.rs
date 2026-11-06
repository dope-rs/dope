use std::convert::Infallible;

/// Completed resources and their undelivered operation result travel together.
pub struct Reclaimed<B, R, E = Infallible> {
    pub buffer: Option<B>,
    pub report: Option<R>,
    pub result: Result<(), E>,
}

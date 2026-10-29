use std::convert::Infallible;

/// Completed resources and their undelivered operation result travel together.
pub struct Reclaimed<B, R, E = Infallible> {
    pub buffer: Option<B>,
    pub report: Option<R>,
    pub result: Result<(), E>,
}

impl<B, R, E> Reclaimed<B, R, E> {
    pub fn map_report<T>(self, map: impl FnOnce(R) -> T) -> Reclaimed<B, T, E> {
        Reclaimed {
            buffer: self.buffer,
            report: self.report.map(map),
            result: self.result,
        }
    }
}

use crate::errors::detail;
use o3::depot::capacity::Shortage;

/// Admission failure while applying peer abandonment reported by an application protocol.
#[derive(Debug)]
pub enum Error {
    Plan(detail::Plan),
    Stream(super::Error),
    Events(Shortage),
}

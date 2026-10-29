use o3::{
    buffer::{hold, resident::staging::reserve, write::consume::scope},
    depot::queue::indexed::shared,
};

/// Why application ingress could not accept one complete transition.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Failure {
    Input(scope::Error),
    Staging(reserve::Error),
    Hold(hold::Error),
    Accept(shared::Bounds),
    FinishCapacity,
    ParseCapacity,
    Contract,
}

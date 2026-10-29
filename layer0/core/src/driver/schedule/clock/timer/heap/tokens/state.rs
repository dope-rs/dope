/// Scheduling state of a leased slot.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum State {
    Idle,
    Armed,
    Fired,
}

/// Scheduling state of a leased slot.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum State<P> {
    Idle,
    Armed(P),
    Fired,
}

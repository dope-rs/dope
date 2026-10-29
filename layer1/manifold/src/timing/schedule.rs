use dope_core::driver::schedule::clock;

/// Driver-branded application scheduling.
///
/// Deadlines retain their original driver lifetime.
pub trait Schedule<'d> {
    fn deadline<'turn>(&self, clock: clock::Clock<'turn, 'd>) -> Option<clock::Deadline<'d>>;
}

impl<'d> Schedule<'d> for clock::Deadline<'d> {
    fn deadline<'turn>(&self, _: clock::Clock<'turn, 'd>) -> Option<clock::Deadline<'d>> {
        Some(*self)
    }
}


use dope_core::driver::schedule::clock::Deadline;
fn widen<'d>(deadline: Deadline<'d>) -> Deadline<'static> {
    deadline
}
fn main() {}

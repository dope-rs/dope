use dope_core::driver::schedule::clock::Deadline;
fn narrow<'d>(deadline: Deadline<'static>) -> Deadline<'d> {
    deadline
}
fn main() {}

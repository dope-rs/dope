use std::task::Poll;

use dope_fiber::abi::machine::Machine;

#[test]
fn terminal_repoll_is_inert() {
    let mut machine = Machine::new(1_u8);
    let first = machine.turn(|_| Poll::Ready(()));
    std::assert_matches!(first, Poll::Ready(()));
    let second = machine.turn(|_| Poll::Ready(()));
    std::assert_matches!(second, Poll::Pending);
}

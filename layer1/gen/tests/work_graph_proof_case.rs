use engine::core::o3::mem::quota;
pub mod common;

use crate::common::graphs::*;

#[test]
fn erases_state_and_rotates() {
    let mut work = WorkGraph::new();
    assert_eq!(WorkGraph::SLOTS, 3);
    assert!(matches!(
        work.select(|candidate| matches!(candidate, Work::Application | Work::Transport)),
        Some(Work::Application)
    ));
    assert!(matches!(
        work.select(|candidate| matches!(candidate, Work::Application | Work::Transport)),
        Some(Work::Transport)
    ));
    work.account(Work::Output);
    assert!(matches!(
        work.select(|candidate| matches!(
            candidate,
            Work::Application | Work::Output | Work::Transport
        )),
        Some(Work::Transport)
    ));
    let admitted = work.admit(|source| match source {
        Work::Application => quota::Admission::Empty,
        Work::Output => quota::Admission::Item(7),
        Work::Transport => quota::Admission::Empty,
    });
    assert!(matches!(admitted, quota::Admission::Item(7)));
    assert!(matches!(work.take(), Work::Transport));
    assert!(matches!(work.take(), Work::Application));
}

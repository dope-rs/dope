#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use dope_core::platform::affinity::Cpu;
use dope_test::alloc::Tracker;

#[test]
fn snapshots_cpus_no_alloc() {
    let (cpus, count) = Tracker::usage(Cpu::current);
    let cpus = cpus.expect("current CPU affinity");

    assert_eq!(count, (0, 0));
    let mut iter = cpus.into_iter();
    assert_ne!(iter.len(), 0);
    let initial = iter.len();
    assert!(iter.next().is_some());
    assert_eq!(iter.len(), initial - 1);
}

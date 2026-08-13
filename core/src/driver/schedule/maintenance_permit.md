Affine proof that one maintenance transition was admitted in this turn.

The permit has no runtime storage. Its invariant driver lifetime prevents a
transition admitted by one driver from being used with another, while the turn
lifetime prevents the proof from escaping the active scheduler turn.

```compile_fail
use dope_core::driver::{self, schedule};

fn escape_driver<'turn, 'outer, 'inner>(
    work: schedule::Maintenance<'turn, 'outer>,
    _inner: driver::Reference<'inner>,
) -> schedule::MaintenancePermit<'turn, 'inner> {
    schedule::MaintenancePermit::try_take(work).unwrap()
}
```

```compile_fail
use dope_core::driver::schedule;

fn escape_turn<'turn, 'd>(
    work: schedule::Maintenance<'turn, 'd>,
) -> schedule::MaintenancePermit<'static, 'd> {
    schedule::MaintenancePermit::try_take(work).unwrap()
}
```

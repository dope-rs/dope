Work capabilities borrowed for one active scheduler turn.

The borrow prevents a capability from surviving the reset at the end of its
turn:

```compile_fail,E0502
use dope_core::driver::schedule;

fn cross_reset(controller: &mut schedule::Controller<'_, '_>) {
    controller.begin(1);
    let work = controller.turn().application();
    controller.end();
    let _ = work.remaining();
}
```

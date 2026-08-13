```compile_fail
use dope_core::driver::{schedule::ready::Key, Reference};

fn cross_driver<'outer, 'inner>(key: Key<'outer>, driver: Reference<'inner>)
where
    'outer: 'inner,
{
    driver.activate_ready(key);
}
```

```compile_fail
use dope_core::driver::Reference;

fn drain(driver: Reference<'_>) {
    driver.drain_ready(drop);
}
```

```compile_fail
use dope_core::driver::{Reference, targets::Token};

fn raw_target(driver: Reference<'_>, target: Token) {
    let _ = driver.make_ready_slot(target);
}
```

```compile_fail
use dope_core::driver::schedule::Controller;

fn recurse(turn: &mut Controller<'_, '_>) {
    turn.drain_ready(1, |_| {
        turn.drain_ready(1, drop);
    });
}
```

```compile_fail
use dope_core::driver::{
    schedule::ready::Slot,
    targets::{KeyTag, OperationTarget},
};

fn cross_route<'d>(
    slot: Slot<'d, KeyTag<1>>,
    target: OperationTarget<'d, KeyTag<2>>,
) {
    slot.set_target(target);
}
```

```compile_fail
use dope_core::driver::{Reference, targets::{KeyTag, OperationTarget}};

fn cross_driver<'a, 'b>(
    driver: Reference<'a>,
    target: OperationTarget<'b, KeyTag<1>>,
) {
    let _ = driver.make_ready_slot(target);
}
```

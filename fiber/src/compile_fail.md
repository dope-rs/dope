# Compile-time contracts

The compiler must reject these violations of fiber ownership. The tests care
only that the boundary holds, not how rustc phrases the diagnostic.

Raw proof modules are never part of the public API:

```compile_fail,E0603
use dope_fiber::raw;
```

Fibers cannot be silently discarded without being driven:

```compile_fail
#![deny(unused_must_use)]

use dope_fiber::abi::Ready;

Ready::new(());
```

The same contract applies through an opaque fiber return:

```compile_fail
#![deny(unused_must_use)]

use dope_fiber::abi::{Fiber, Ready};

fn opaque<'d>() -> impl Fiber<'d, Output = ()> {
    Ready::new(())
}

opaque();
```

A local-cell borrow cannot escape its callback:

```compile_fail
use dope_fiber::task::local::{Cell, Context};

fn escape<'d>(cell: &Cell<'d, String>, local: &Context<'_, 'd>) -> &'d str {
    cell.read_with(local, |value| value.as_str())
}
```

Completion wakers are linear capabilities:

```compile_fail,E0382
use dope::core::driver::schedule::ready::completion::Waker;

fn consume(_: Waker<'_>) {}

fn duplicate(wake: Waker<'_>) {
    consume(wake);
    consume(wake);
}
```

A completion waker cannot be widened beyond its driver domain:

```compile_fail
use std::pin::Pin;
use dope::core::driver::schedule::ready::completion;
use dope_fiber::context::Context;

fn widen<'d>(context: Pin<&Context<'_, 'd>>) -> completion::Waker<'static> {
    context.completion_waker()
}
```

Root authority cannot be retagged into another driver lifetime:

```compile_fail
use dope_fiber::context::RootWaker;

fn retag<'left, 'right>(wake: RootWaker<'left>) -> RootWaker<'right> {
    wake
}
```

Root authority is not an owner-identity comparison token:

```compile_fail,E0369
use dope_fiber::context::RootWaker;

fn same<'d>(left: RootWaker<'d>, right: RootWaker<'d>) -> bool {
    left == right
}
```

Task wakers require their raw binding proof:

```compile_fail,E0133
use std::pin::Pin;
use dope::core::driver::schedule::ready::task::{Node, raw::wake::NodeBinding};

fn extract<'d>(node: Pin<&Node<'d>>) {
    let _ = NodeBinding::waker(node);
}
```

Fiber identifiers remain thread-confined:

```compile_fail,E0277
use dope_fiber::task::storage::Id;

fn require_send<T: Send>() {}
require_send::<Id<'static>>();
```

```compile_fail,E0277
use dope_fiber::task::storage::Id;

fn require_sync<T: Sync>() {}
require_sync::<Id<'static>>();
```

```compile_fail,E0277
use dope_fiber::task::storage::RoutedId;

fn require_send<T: Send>() {}
require_send::<RoutedId<'static, (), 0, ()>>();
```

```compile_fail,E0277
use dope_fiber::task::storage::RoutedId;

fn require_sync<T: Sync>() {}
require_sync::<RoutedId<'static, (), 0, ()>>();
```

Fiber identifiers cannot outlive their driver domain:

```compile_fail
use dope_fiber::task::storage::Id;

fn escape<'d>(id: Id<'d>) -> Id<'static> {
    id
}
```

Route owners and domains cannot be exchanged:

```compile_fail,E0308
use dope_fiber::task::storage::{Id, RoutedTag};

enum Left {}
enum Right {}

fn rebrand<'d>(
    id: Id<'d, RoutedTag<Left, 1, 7>>,
) -> Id<'d, RoutedTag<Right, 1, 7>> {
    id
}
```

```compile_fail,E0308
use dope_fiber::task::storage::{Id, RoutedTag};

enum Owner {}

fn cross_domain<'d>(
    id: Id<'d, RoutedTag<Owner, 1, 7>>,
) -> Id<'d, RoutedTag<Owner, 2, 7>> {
    id
}
```

A task waker cannot be stored beyond the fiber poll:

```compile_fail,E0308
use std::{cell::Cell, task::Poll};
use dope_fiber::{abi::Fiber, context::{PollCall, Waker}};

struct Escape<'d>(&'d Cell<Option<Waker<'d>>>);

impl<'d> Fiber<'d> for Escape<'d> {
    type Output = ();

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<()> {
        let (this, cx) = call.into_parts();
        this.0.set(Some(cx.waker()));
        Poll::Pending
    }
}
```

Application code cannot synthesize a fiber-poll admission:

```compile_fail,E0624
use std::pin::Pin;
use dope_fiber::{abi::Ready, context::{Context, PollCall}};

fn forge<'call, 'turn, 'd: 'turn>(
    fiber: Pin<&'call mut Ready<()>>,
    context: Pin<&'call mut Context<'turn, 'd>>,
) -> PollCall<'call, 'turn, 'd, Ready<()>> {
    PollCall::new(fiber, context)
}
```

A poll admission is tied to its exact pinned fiber type:

```compile_fail,E0308
use dope_fiber::{abi::{Pending, Ready}, context::PollCall};

fn retag<'call, 'turn, 'd>(
    call: PollCall<'call, 'turn, 'd, Ready<()>>,
) -> PollCall<'call, 'turn, 'd, Pending<()>> {
    call
}
```

Driver-owned timers cannot be constructed by applications:

```compile_fail,E0624
use dope::core::driver::schedule::timer::Timer;

let _ = Timer::with_capacity;
```

A retained read lease cannot outlive the `Io` borrow that produced it:

```compile_fail
use dope::net::wire::Wire;
use dope_fiber::net::ReadLease;

fn widen<'io, 'd, W: Wire + 'd>(
    lease: ReadLease<'io, 'd, W>,
) -> ReadLease<'static, 'd, W> {
    lease
}
```

A scheduler cannot be driven with work and retained access from another
driver domain:

```compile_fail
use dope::core::driver::{self, schedule};
use dope_fiber::{abi::Fiber, task::Scheduler};

fn cross_driver<'left, 'right, F>(
    scheduler: &mut Scheduler<'left, F>,
    work: schedule::Application<'_, 'right>,
    driver: &mut driver::retained::Context<'_, '_, 'right>,
) where
    F: Fiber<'left>,
{
    scheduler.drive_ready(work, driver, |_, _| {});
}
```

Fiber storage cannot be polled without the scheduler's application-work
permit:

```compile_fail,E0599
use dope_fiber::{abi::Ready, task::storage::Slab};

let _ = Slab::<'static, Ready<()>>::poll;
```

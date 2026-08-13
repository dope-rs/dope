# Compile-time contracts

The macro front-end intentionally rejects constructs it cannot lower while
preserving pinning and borrow guarantees. These tests assert rejection without
freezing rustc's rendered diagnostics.

The runtime crate path is explicit for both public fiber entry points:

```compile_fail
let _ = dope_gen::fiber!('_ => async move {});
```

```compile_fail
#[dope_gen::fiber_fn('d)]
async fn run<'d>() {}
```

Nested async closures are not fiber scopes:

```compile_fail
use fiber_rt::abi::Ready;

let _ = dope_gen::fiber!('_, crate = ::fiber_rt => async move {
    let _ = async || Ready::new(()).await;
});
```

`fiber_fn` cannot hide another async scope:

```compile_fail
#[dope_gen::fiber_fn('d, crate = ::fiber_rt)]
async fn run<'d>() {
    let _ = async {};
}
```

Nested async items and blocks cannot escape the lowering boundary:

```compile_fail
use fiber_rt::abi::Ready;

let _ = dope_gen::fiber!('_, crate = ::fiber_rt => async move {
    async fn escaped() { Ready::new(()).await; }
    trait Trait { async fn escaped(); }
    struct Type;
    impl Trait for Type {
        async fn escaped() { Ready::new(()).await; }
    }
    let _ = async { Ready::new(()).await };
});
```

Captured owners cannot yield references with the driver lifetime:

```compile_fail,E0597
use fiber_rt::abi::Ready;

fn leak<'d>(text: String) -> impl fiber_rt::abi::Fiber<'d, Output = &'d str> {
    dope_gen::fiber!('d, crate = ::fiber_rt => async move {
        Ready::new((&text).as_str()).await
    })
}
```

Fiber-local owners cannot yield dangling references:

```compile_fail,E0515
use fiber_rt::abi::Ready;

fn leak<'d>() -> impl fiber_rt::abi::Fiber<'d, Output = &'d str> {
    dope_gen::fiber!('d, crate = ::fiber_rt => async move {
        let text = Ready::new(String::from("owned")).await;
        let borrowed = Ready::new(text.as_str()).await;
        let _owner_remains_live = text.len();
        borrowed
    })
}
```

Control flow cannot escape through a nested macro expansion:

```compile_fail,E0308
let _ = dope_gen::fiber!('_, crate = ::fiber_rt => async move {
    ::core::matches!(return 1usize, _)
});
```

```compile_fail,E0277,E0308
let _ = dope_gen::fiber!('_, crate = ::fiber_rt => async move {
    ::core::matches!(Ok::<usize, ()>(1)?, Ok(_))
});
```

Forwarding derives reject manual `Unpin` implementations:

```compile_fail,E0119
#[pin_project::pin_project]
#[derive(dope_gen::Forward)]
struct App<'d> {
    #[pin]
    #[forward]
    inner: dope::manifold::timing::Interval<'d, 0>,
}

impl Unpin for App<'_> {}
```

The `Forward` parser accepts exactly one structurally pinned `#[forward]`
field. `#[forward('d, capability = C)]` may name one shared capability when
all four forwarded lifecycle capabilities are `C`; the generated method calls
still type-check that declaration against the inner manifold while keeping its
private concrete type out of the wrapper's public associated types. The
`Application` parser likewise requires every retained owner to be a
structurally pinned `#[manifold]` field and every other field to be either
`'static` state, `'static` scheduled state, or the invariant driver marker. These syntax contracts are
tested directly at the parser boundary so unrelated type errors cannot satisfy
them.

Application state cannot be structurally pinned:

```compile_fail
#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct App {
    #[pin]
    #[dispatcher(state)]
    state: usize,
}
```

Application state must be `'static`, so a retained owner carrying the
generative driver lifetime cannot bypass lifecycle traversal through an
otherwise unpinned field:

```compile_fail,E0277
use std::pin::Pin;

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct App<'d> {
    #[dispatcher(state)]
    hidden_owner: Pin<Box<dope::manifold::timing::Interval<'d, 9>>>,
    #[dispatcher(marker)]
    driver: ::core::marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

fn require_application<'d>(app: &App<'d>) {
    fn require<'d>(_: &impl dope::runtime::executor::Application<'d>) {}
    require(app);
}
```

Application markers cannot also claim lifecycle ownership. This mutually
exclusive parser role is checked directly, before code generation or type
checking.

Each dispatcher field has exactly one recognized role:

```compile_fail
#[derive(dope_gen::Application)]
struct App {
    #[dispatcher(state, marker)]
    ambiguous: usize,
}
```

```compile_fail
#[derive(dope_gen::Application)]
struct App {
    #[dispatcher(unknown)]
    unknown: usize,
}
```

```compile_fail
#[derive(dope_gen::Application)]
struct App {
    #[dispatcher(state)]
    #[dispatcher(schedule)]
    duplicated: usize,
}
```

Application markers must be an absolute, invariant core `PhantomData`:

```compile_fail
#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct App {
    #[dispatcher(marker)]
    marker: std::marker::PhantomData<()>,
}
```

A covariant marker cannot silently select a dispatcher lifetime:

```compile_fail
#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct App<'d> {
    #[dispatcher(marker)]
    marker: ::core::marker::PhantomData<&'d ()>,
}
```

Multiple lifetime parameters require an explicit driver marker instead of
implicitly treating the first lifetime as the driver lifetime:

```compile_fail
#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct App<'scope, 'driver> {
    #[dispatcher(state)]
    scope: ::core::marker::PhantomData<&'scope ()>,
    #[dispatcher(state)]
    driver: ::core::marker::PhantomData<&'driver ()>,
}
```

Application markers cannot be structurally pinned:

```compile_fail
#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct App {
    #[pin]
    #[dispatcher(marker)]
    marker: ::core::marker::PhantomData<fn(&'static ()) -> &'static ()>,
}
```

Coordination hooks receive only the generated step-scoped control projection,
not the application root or a raw driver context:

```compile_fail,E0308
#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[coordinate]
struct App {}

impl App {
    fn coordinate<'d>(
        &mut self,
        _: &mut dope::core::driver::Context<'_, 'd>,
    ) {
    }
}
```

The region authority inside a coordination step cannot be widened to the
generative driver lifetime:

```compile_fail
fn leak_region<'step, 'turn, 'd: 'step>(
    step: &'step mut dope::runtime::coordinate::Step<'step, 'turn, 'd>,
) -> &'d mut o3::cell::region::Token<'d> {
    step.region()
}
```

A control can issue only the commands selected by its Manifold. It cannot be
used with `Pin::set` to replace the installed owner:

```compile_fail,E0599
#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[coordinate]
struct App<'d> {
    #[pin]
    #[manifold(control)]
    interval: dope::manifold::timing::Interval<'d, 7>,
    #[dispatcher(marker)]
    driver: ::core::marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d> App<'d> {
    fn coordinate(
        mut this: AppCoordinate<'_, '_, 'd>,
    ) -> dope::runtime::coordinate::Flow {
        let replacement: dope::manifold::timing::Interval<'d, 7> = panic!();
        this.interval.set(replacement);
        dope::runtime::coordinate::Flow::Idle
    }
}
```

Application derives do not publish mutable pinned projections that let callers
replace a live Manifold owner:

```compile_fail,E0599
use std::pin::Pin;

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct App<'d> {
    #[pin]
    #[manifold]
    interval: dope::manifold::timing::Interval<'d, 7>,
}

fn replace<'d>(
    mut app: Pin<&mut App<'d>>,
    replacement: dope::manifold::timing::Interval<'d, 7>,
) {
    app.as_mut().interval_pin().set(replacement);
}
```

Packed structs cannot receive pin-projecting derives:

```compile_fail
#[repr(packed)]
#[derive(dope_gen::Application)]
struct App {
    #[manifold]
    inner: (),
}
```

The standard `pin!` macro is not a supported lowered statement:

```compile_fail
use fiber_rt::abi::Ready;

let _ = dope_gen::fiber!('_, crate = ::fiber_rt => async move {
    let _value = ::std::pin::pin!(Ready::new(()));
});
```

Only fibers, not arbitrary standard futures, may be awaited:

```compile_fail,E0277
let _ = dope_gen::fiber!('_, crate = ::fiber_rt => async move {
    std::future::ready(()).await;
});
```

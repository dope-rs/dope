# Compile-time contracts

These examples verify egress ownership without pinning compiler diagnostics.

A detached queue front keeps its region token exclusively borrowed:

```compile_fail,E0499
use dope_net::link::egress::metadata;
use o3::cell::region;

fn use_region(_: &mut region::Token<'_>) {}

fn reenter<'d>(token: &mut region::Token<'d>, queue: metadata::Queue<'_, 'd, &'static [u8]>) {
    let (_, front) = queue.take_front(token).unwrap();
    use_region(token);
    front.release();
}
```

Application types cannot implement the sealed payload contract:

```compile_fail,E0277
use dope_net::link::egress::data::Payload;

struct ApplicationBytes(Vec<u8>);

impl AsRef<[u8]> for ApplicationBytes {
    fn as_ref(&self) -> &[u8] { &self.0 }
}

impl Payload for ApplicationBytes {}
```

A borrowed payload cannot enter the zero-retention path:

```compile_fail
use dope_net::link::egress::data::Buffer;

fn borrowed(bytes: &[u8]) -> Buffer<'static> {
    Buffer::Borrowed(bytes)
}
```

An arbitrary borrowed slice is not a retained direct-send source:

```compile_fail,E0599
use dope_net::wire::send::Plain;

fn borrowed(bytes: &[u8]) {
    let _ = Plain::from_slice(bytes);
}
```

An `OnSubmit` wire cannot return input bytes that belong to the caller's
queue. Only `OnComplete` exposes this constructor:

```compile_fail,E0599
use dope_net::wire::{reclaim::OnSubmit, send::{Plain, Prepared}};

fn reclaim_input<'a>(plain: Plain<'a>) -> Prepared<'a, OnSubmit> {
    Prepared::<OnSubmit>::input(plain)
}
```

The same policy boundary applies to vectored input and its descriptor storage:

```compile_fail,E0599
use dope_net::wire::{reclaim::OnSubmit, send::{Prepared, Vectored}};

fn reclaim_vectored<'a>(plain: Vectored<'a>) -> Prepared<'a, OnSubmit> {
    Prepared::<OnSubmit>::vectored(plain)
}
```

An exact retained input cannot claim a different consumed length:

```compile_fail,E0061
use dope_net::wire::{reclaim::OnComplete, send::{Plain, Prepared}};

fn shorten<'a>(plain: Plain<'a>) -> Prepared<'a, OnComplete> {
    Prepared::<OnComplete>::input(plain, 0)
}
```

Connection-owned transformed output is an `OnSubmit` preparation and cannot
be relabeled as exact retained input:

```compile_fail,E0308
use dope_net::wire::{
    reclaim::OnComplete,
    send::{Buffer, Prepared, Storage},
};

fn transform<'a>(send: Storage<'a, Buffer<8>>) -> Prepared<'a, OnComplete> {
    send.buffered(1)
}
```

An `OnComplete` transition is terminal. It cannot chain independent output
whose wire-byte completion would no longer identify the retained input:

```compile_fail,E0599
use dope_net::wire::{reclaim::OnComplete, send::{Prepared, Transition}};

fn chain<'a>(prepared: Prepared<'a, OnComplete>) -> Transition<'a, OnComplete> {
    Transition::<OnComplete>::unchanged(prepared)
}
```

An arbitrary safe iterator cannot claim the receive batch proof:

```compile_fail,E0277
use dope_net::wire::batch::raw::Source;

struct Liar;

impl Iterator for Liar {
    type Item = ();

    fn next(&mut self) -> Option<()> {
        Some(())
    }
}

impl ExactSizeIterator for Liar {
    fn len(&self) -> usize {
        0
    }
}

fn require<T: Source>() {}

fn rejected() {
    require::<Liar>();
}
```

Retained link storage cannot be authorized by forging a free-standing owner;
the pool instead takes ownership of its route:

```compile_fail,E0432
use dope_net::link::raw::Owner;
```

A committed route is linear and therefore cannot activate two retained pools:

```compile_fail,E0382
use dope_core::driver::lifecycle::routing::Route;
use dope_net::{Transport, link::pool::Prepared, wire::Wire};

fn bind_twice<'d, const ID: u8, T, W, S>(
    first: Prepared<'d, ID, T, W, S>,
    second: Prepared<'d, ID, T, W, S>,
    route: Route<'d, ID>,
) where
    T: Transport,
    W: Wire,
{
    let _first = unsafe { first.bind(route) };
    let _second = unsafe { second.bind(route) };
}
```

Raw pool construction is an explicit proof boundary; safe code cannot obtain a
pool whose operational methods would precede pinned runtime installation:

```compile_fail,E0133
use dope_core::driver::lifecycle::routing::Route;
use dope_net::{Transport, link::pool::Prepared, wire::Wire};

fn bind_without_lifecycle<'d, const ID: u8, T, W, S>(
    prepared: Prepared<'d, ID, T, W, S>,
    route: Route<'d, ID>,
)
where
    T: Transport,
    W: Wire,
{
    let _pool = prepared.bind(route);
}
```

Inbound storage has no outbound descriptor capability and therefore cannot
construct a dialer:

```compile_fail,E0599
use dope_net::{link::pool::Pool, tcp::Tcp, wire::Identity};

fn dial_inbound(pool: &mut Pool<'_, 0, Tcp, Identity, ()>) {
    let _ = pool.dialer();
}
```

An accepted socket can enter the tuning state only through its inbound pool;
safe consumers cannot construct the internal engine phase directly:

```compile_fail,E0624
use dope_core::io::fd::handles::Descriptor;
use dope_net::link::Engine;

fn bypass<'d>(fd: Descriptor<'d>) {
    let _ = Engine::from_accepted_tuning(fd);
}
```

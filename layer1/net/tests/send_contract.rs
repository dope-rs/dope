pub mod common;

use common::wire::Buffer;
use dope_net::{link, wire};
use dope_test::alloc::Tracker;
use o3::buffer::write::Sink as _;
use reclaim::Policy as _;
use wire::send::{self, reclaim};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn completed_is_empty() {
    let mut store = ();
    let state = send::Transition::completed(send::Storage::new(&mut store));
    let (ready, avail) = state.into_parts();
    let (body, used, close) = ready.into_parts();

    assert!(matches!(body, send::Empty));
    assert!(reclaim::OnComplete::immediate(used) == dope_core::io::transfer::Len::ZERO);
    assert!(close == send::End::Keep);
    assert!(avail == send::Availability::Unchanged);
}

#[test]
fn prepared_layouts() {
    use std::mem::size_of;

    type Empty = send::Prepared<'static, reclaim::OnSubmit, send::Empty>;
    type Plain = send::Prepared<'static, reclaim::OnComplete, send::Plain<'static>>;
    type Vectored = send::Prepared<'static, reclaim::OnComplete, send::Vectored<'static>>;

    assert_eq!(
        size_of::<link::Consumed>(),
        size_of::<dope_core::io::transfer::Len>()
    );
    assert_eq!(size_of::<Empty>(), size_of::<u64>());
    assert_eq!(size_of::<Plain>(), size_of::<send::Plain<'static>>() + 8);
    assert_eq!(
        size_of::<Vectored>(),
        size_of::<send::Vectored<'static>>() + 8
    );
}

#[test]
fn send_paths_no_alloc() {
    let mut store = Buffer::<64>::default();
    assert!(store.0.write_slice(b"retained").is_ok());

    let (state, alloc) = Tracker::usage(|| {
        let ready = send::Storage::new(&mut store)
            .consume_none()
            .buffered()
            .close_after();
        let (body, used, close) = ready.into_parts();
        (body.is_empty(), used, close)
    });

    assert!(state == (false, dope_core::io::transfer::Len::ZERO, send::End::Close));
    assert_eq!(alloc, (0, 0));
}

#[test]
fn reclaim_selects_rules() {
    assert!(reclaim::OnSubmit::direct() == reclaim::Direct::Independent);
    assert!(reclaim::OnComplete::direct() == reclaim::Direct::Retained);
}

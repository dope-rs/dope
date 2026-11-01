pub mod common;

use common::wire::contract::Case;
use dope_net::{link, wire};
use dope_test::alloc::Tracker;
use reclaim::Policy as _;
use wire::send::{self, reclaim};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

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
    let (bytes, queued, closing) = Case::Buffered.run();
    assert_eq!(bytes, b"retained");
    assert!(queued, "buffered output must not consume queued input");
    assert!(closing, "prepared close intent must reach the connection");
}

#[test]
fn reclaim_selects_rules() {
    assert!(reclaim::OnSubmit::direct() == reclaim::Direct::Independent);
    assert!(reclaim::OnComplete::direct() == reclaim::Direct::Retained);
}

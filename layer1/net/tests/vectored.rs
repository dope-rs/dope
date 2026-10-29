pub mod common;

use common::wire::Buffer;
use dope_core::{
    dope_sys::io::socket::msg::{Header, Iovec},
    io::{socket::msg, transfer},
};
use dope_net::wire::send::{self, progress::Plan};
use dope_test::alloc::Tracker;
use msg::raw::{Iovec as _, Vectored as _};
use o3::buffer::write::Sink as _;
use o3::depot::array::copy;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn plan_keeps_owners() {
    let bytes = [b"ab".as_slice(), b"cdef".as_slice()];
    let mut iovecs = copy::Vec::<_, 8>::new();
    for iovec in [
        Iovec::empty(),
        unsafe { bytes[0].retain() },
        Iovec::empty(),
        unsafe { bytes[1].retain() },
        Iovec::empty(),
    ] {
        iovecs.vacancy().expect("five iovecs fit").insert(iovec);
    }
    let mut header = Header::new();
    let mut buffer = Buffer::<8>::default();
    let (output, alloc) = Tracker::usage(|| {
        let mut plan = {
            let graph = unsafe { (&mut header, &iovecs, transfer::Len::fixed::<6>()).retain() };
            let input: send::Vectored<'_, 8> = send::Vectored::from_message(graph);
            Plan::vectored(send::Storage::new(&mut buffer), &input)
        };
        {
            let (store, mut input) = plan.parts();
            assert!(input.take(transfer::Len::MAX).commit().is_zero());
            let mut retry = input.take(transfer::Len::MAX);
            assert_eq!(retry.take().next().expect("first chunk").bytes(), b"ab");
            drop(retry);
            assert_eq!(input.remaining(), transfer::Len::fixed::<6>());
            let mut first = input.take(transfer::Len::MAX);
            store
                .0
                .write_slice(first.take().next().expect("same first chunk").bytes())
                .expect("bounded send storage");
            assert_eq!(first.commit(), transfer::Len::fixed::<2>());
        }
        for limit in [transfer::Len::fixed::<1>(), transfer::Len::MAX] {
            let (store, mut input) = plan.parts();
            let mut parts = input.take(limit);
            for part in parts.take() {
                store
                    .0
                    .write_slice(part.bytes())
                    .expect("bounded send storage");
            }
            parts.commit();
        }
        let (_, input) = plan.parts();
        assert!(input.is_empty());
        assert!(input.remaining() == transfer::Len::ZERO);
        let (body, used, end) = plan.finish().buffered().close_after().into_parts();
        (body.len(), used, end)
    });
    assert!(
        output
            == (
                transfer::Len::fixed::<6>(),
                transfer::Len::fixed::<6>(),
                send::End::Close
            )
    );
    assert_eq!(buffer.0.as_ref(), b"abcdef");
    assert_eq!(alloc, (0, 0));
}

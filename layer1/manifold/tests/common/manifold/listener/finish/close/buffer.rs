use dope_core::io::transfer;
use dope_net::wire::send::{self, progress::Plan, reclaim};
use o3::buffer::write::Sink as _;

use crate::common;

pub(crate) fn plain<'a, const CAP: usize>(
    send: send::Storage<'a, common::Buffer<CAP>>,
    plain: send::Plain<'a>,
) -> send::Prepared<'a, reclaim::OnSubmit, send::Plain<'a>> {
    let mut plan = Plan::plain(send, plain);
    let (store, mut input) = plan.parts();
    let part = input.take(transfer::Len::prefix(
        store.bytes.available(),
        transfer::Len::MAX,
    ));
    store
        .bytes
        .write_slice(part.as_slice())
        .expect("buffered plain input");
    part.commit();
    plan.finish()
}

pub(crate) fn vectored<'a, const CAP: usize, const N: usize>(
    send: send::Storage<'a, common::Buffer<CAP>>,
    plain: send::Vectored<'a, N>,
) -> send::Prepared<'a, reclaim::OnSubmit, send::Plain<'a>> {
    let mut plan = Plan::vectored(send, &plain);
    let (store, mut input) = plan.parts();
    let mut parts = input.take(transfer::Len::prefix(
        store.bytes.available(),
        input.remaining(),
    ));
    for part in parts.take() {
        store
            .bytes
            .write_slice(part.bytes())
            .expect("buffered vectored input");
    }
    parts.commit();
    plan.finish()
}

pub(crate) fn complete<'a, const CAP: usize>(
    done: send::Completion<'a, reclaim::OnSubmit, common::Buffer<CAP>>,
) -> send::Transition<'a, reclaim::OnSubmit, send::Plain<'a>> {
    let send = done.consume_max();
    send::Transition::unchanged(send.buffered())
}

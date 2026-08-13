use dope_core::driver::{
    lifecycle::routing::{Reserved, Route},
    ops,
    route::KeyTag,
    schedule::timer::{Registration, Timer},
};
use dope_test::checks::affinities::Affinity;

type Submission = dope_core::driver::retained::raw::Submission<'static, 'static, KeyTag<1>>;

const _: fn() = || {
    Affinity::<dope_core::driver::Driver>::not_send::<_>();
    Affinity::<dope_core::driver::Driver>::not_sync::<_>();
    Affinity::<dope_core::driver::Context<'static, 'static>>::not_send::<_>();
    Affinity::<dope_core::driver::Context<'static, 'static>>::not_sync::<_>();
    Affinity::<dope_core::driver::lifecycle::quiesce::Final<'static, 'static>>::not_send::<_>();
    Affinity::<dope_core::driver::lifecycle::quiesce::Final<'static, 'static>>::not_sync::<_>();
    Affinity::<dope_core::driver::Reference<'static>>::not_send::<_>();
    Affinity::<dope_core::driver::Reference<'static>>::not_sync::<_>();
    Affinity::<dope_core::driver::schedule::Controller<'static, 'static>>::not_send::<_>();
    Affinity::<dope_core::driver::schedule::Controller<'static, 'static>>::not_sync::<_>();
    Affinity::<dope_core::driver::schedule::ActiveTurn<'static, 'static>>::not_send::<_>();
    Affinity::<dope_core::driver::schedule::ActiveTurn<'static, 'static>>::not_sync::<_>();
    Affinity::<Route<'static, 0>>::not_send::<_>();
    Affinity::<Route<'static, 0>>::not_sync::<_>();
    Affinity::<Reserved<'static, 0>>::not_send::<_>();
    Affinity::<Reserved<'static, 0>>::not_sync::<_>();
    Affinity::<dope_core::driver::lifecycle::Install<'static, 'static>>::not_send::<_>();
    Affinity::<dope_core::driver::lifecycle::Install<'static, 'static>>::not_sync::<_>();
    Affinity::<dope_core::driver::lifecycle::Finalize<'static, 'static>>::not_send::<_>();
    Affinity::<dope_core::driver::lifecycle::Finalize<'static, 'static>>::not_sync::<_>();
    Affinity::<dope_core::io::Event<'static>>::not_send::<_>();
    Affinity::<dope_core::io::Event<'static>>::not_sync::<_>();
    Affinity::<dope_core::io::recv::Lease<'static>>::not_send::<_>();
    Affinity::<dope_core::io::recv::Lease<'static>>::not_sync::<_>();
    Affinity::<dope_core::io::recv::View<'static>>::not_send::<_>();
    Affinity::<dope_core::io::recv::View<'static>>::not_sync::<_>();
    Affinity::<ops::OutboundReservation<'static, 0>>::not_send::<_>();
    Affinity::<ops::OutboundReservation<'static, 0>>::not_sync::<_>();
    Affinity::<dope_core::io::fs::Directory>::not_send::<_>();
    Affinity::<dope_core::io::fs::Directory>::not_sync::<_>();
    Affinity::<Registration<'static, 'static>>::not_send::<_>();
    Affinity::<Registration<'static, 'static>>::not_sync::<_>();
    Affinity::<Timer<'static>>::not_send::<_>();
    Affinity::<Timer<'static>>::not_sync::<_>();
    Affinity::<Submission>::not_send::<_>();
    Affinity::<Submission>::not_sync::<_>();
    Affinity::<dope_core::io::fd::handles::Descriptor<'static>>::not_send::<_>();
    Affinity::<dope_core::io::fd::handles::Descriptor<'static>>::not_sync::<_>();
    Affinity::<dope_core::io::fd::handles::SocketSlot<'static>>::not_send::<_>();
    Affinity::<dope_core::io::fd::handles::SocketSlot<'static>>::not_sync::<_>();
    Affinity::<dope_core::io::fd::handles::CreatingSocket<'static>>::not_send::<_>();
    Affinity::<dope_core::io::fd::handles::CreatingSocket<'static>>::not_sync::<_>();
    Affinity::<dope_core::io::fd::handles::CreatedSlot<'static>>::not_send::<_>();
    Affinity::<dope_core::io::fd::handles::CreatedSlot<'static>>::not_sync::<_>();
    Affinity::<dope_core::driver::route::Token>::not_send::<_>();
    Affinity::<dope_core::driver::route::Token>::not_sync::<_>();
    Affinity::<dope_core::driver::route::Epoch>::not_send::<_>();
    Affinity::<dope_core::driver::route::Epoch>::not_sync::<_>();
    Affinity::<dope_core::driver::schedule::ready::Key>::not_send::<_>();
    Affinity::<dope_core::driver::schedule::ready::Key>::not_sync::<_>();
    Affinity::<dope_core::driver::schedule::ready::completion::Waker<'static>>::not_send::<_>();
    Affinity::<dope_core::driver::schedule::ready::completion::Waker<'static>>::not_sync::<_>();
    Affinity::<dope_core::driver::schedule::ready::completion::Slot<'static>>::not_send::<_>();
    Affinity::<dope_core::driver::schedule::ready::completion::Slot<'static>>::not_sync::<_>();
};

#[test]
fn local_capabilities_keep_their_layout() {
    let gso_control_layout = if dope_core::io::datagram::GSO_LIMITS.is_some() {
        (40, 8)
    } else {
        (0, 1)
    };
    assert_eq!(
        size_of::<dope_core::io::datagram::GsoControl>(),
        gso_control_layout.0
    );
    assert_eq!(
        align_of::<dope_core::io::datagram::GsoControl>(),
        gso_control_layout.1
    );
    assert_eq!(
        size_of::<Option<dope_core::io::datagram::GsoControl>>(),
        gso_control_layout.0
    );
    assert_eq!(size_of::<Route<'static, 0>>(), 2 * size_of::<usize>());
    assert_eq!(size_of::<Reserved<'static, 0>>(), size_of::<usize>());
    assert_eq!(
        size_of::<dope_core::driver::schedule::Controller<'static, 'static>>(),
        2 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<dope_core::driver::schedule::ActiveTurn<'static, 'static>>(),
        2 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<dope_core::io::fd::handles::Descriptor<'static>>(),
        2 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<dope_core::driver::schedule::ready::Key>(),
        size_of::<u64>()
    );
    assert_eq!(
        size_of::<
            dope_core::driver::schedule::ready::Slot<'static, dope_core::driver::route::KeyTag<1>>,
        >(),
        2 * size_of::<u64>()
    );
    assert_eq!(
        size_of::<dope_core::driver::schedule::ready::completion::Waker<'static>>(),
        2 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<dope_core::driver::schedule::ready::task::Admission<'static, 'static, 'static>>(),
        4 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<dope_core::driver::schedule::ready::task::raw::Binding<'static>>(),
        5 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<dope_core::driver::schedule::ready::completion::Slot<'static>>(),
        2 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<dope_core::driver::lifecycle::quiesce::Final<'static, 'static>>(),
        size_of::<dope_core::driver::Context<'static, 'static>>()
    );
    assert_eq!(
        size_of::<dope_core::driver::lifecycle::Install<'static, 'static>>(),
        0
    );
    assert_eq!(
        size_of::<dope_core::driver::lifecycle::Finalize<'static, 'static>>(),
        size_of::<dope_core::driver::Context<'static, 'static>>()
    );
    assert_eq!(
        size_of::<ops::OutboundReservation<'static, 0>>(),
        2 * size_of::<usize>()
    );
}

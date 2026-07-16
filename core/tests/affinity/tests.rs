use dope_test::{not_send, not_sync, require_send};

const _: fn() = || {
    not_send::<dope_core::backend::Sqe, _>();
    not_sync::<dope_core::backend::Sqe, _>();
    not_send::<dope_core::io::fd::Fd<'static>, _>();
    not_sync::<dope_core::io::fd::Fd<'static>, _>();
    not_send::<dope_core::driver::token::Token, _>();
    not_sync::<dope_core::driver::token::Token, _>();
    not_send::<dope_core::io::fd::FdSlot, _>();
    not_sync::<dope_core::io::fd::FdSlot, _>();
    not_send::<dope_core::driver::ready::ReadyKey, _>();
    not_sync::<dope_core::driver::ready::ReadyKey, _>();
    not_sync::<dope_core::io::pipe::Pipe, _>();
};

#[test]
fn external_pipe_is_send() {
    require_send::<dope_core::io::pipe::Pipe>();
}

#[test]
fn local_capabilities_keep_their_layout() {
    assert_eq!(size_of::<dope_core::io::fd::FdSlot>(), size_of::<u32>());
    assert_eq!(
        size_of::<dope_core::driver::ready::ReadyKey>(),
        size_of::<u64>()
    );
    assert_eq!(
        size_of::<dope_core::driver::ready::ReadySlot<'static>>(),
        3 * size_of::<u64>()
    );
}

use dope_test::checks::{TrackingAlloc, affinities::Affinity};

#[global_allocator]
static ALLOCATOR: TrackingAlloc = TrackingAlloc::new();

const _: fn() = || {
    Affinity::<dope_runtime::executor::Executor>::not_send::<_>();
    Affinity::<dope_runtime::executor::Executor>::not_sync::<_>();
    Affinity::<dope_runtime::executor::session::Session<'static, 'static>>::not_send::<_>();
    Affinity::<dope_runtime::executor::session::Session<'static, 'static>>::not_sync::<_>();
    Affinity::<dope_runtime::process::Context>::not_send::<_>();
    Affinity::<dope_runtime::process::Context>::not_sync::<_>();
    Affinity::<dope_runtime::process::Runtime<()>>::require_send();
    Affinity::<dope_runtime::process::Control>::require_send();
    Affinity::<dope_runtime::process::Shutdown>::require_send();
    Affinity::<dope_runtime::shutdown::Pair>::require_send();
    Affinity::<dope_runtime::shutdown::Source>::require_send();
    Affinity::<dope_runtime::shutdown::Trigger>::require_send();
    Affinity::<dope_runtime::shutdown::Requested>::not_send::<_>();
    Affinity::<dope_runtime::shutdown::Requested>::not_sync::<_>();
    Affinity::<dope_runtime::random::HashState<'static>>::not_send::<_>();
    Affinity::<dope_runtime::random::HashState<'static>>::not_sync::<_>();
    Affinity::<dope_runtime::random::Hasher<'static>>::not_send::<_>();
    Affinity::<dope_runtime::random::Hasher<'static>>::not_sync::<_>();
    Affinity::<dope_runtime::executor::raw::ShutdownRoot<'static, 'static, 'static, ()>>::not_send::<
        _,
    >();
    Affinity::<dope_runtime::executor::raw::ShutdownRoot<'static, 'static, 'static, ()>>::not_sync::<
        _,
    >();
    Affinity::<dope_runtime::executor::raw::Pending<'static, 'static, ()>>::not_send::<_>();
    Affinity::<dope_runtime::executor::raw::Pending<'static, 'static, ()>>::not_sync::<_>();
};

#[test]
fn available_cpus_are_snapshotted_without_allocation() -> std::io::Result<()> {
    use dope_runtime::process::Cpus;

    let (cpus, allocation) = TrackingAlloc::<0>::measure(Cpus::current);
    let mut cpus = cpus?;

    assert_eq!(allocation, (0, 0));
    assert_ne!(cpus.len(), 0);
    let initial = cpus.len();
    let _ = cpus.next();
    assert_eq!(cpus.len(), initial - 1);
    Ok(())
}

#[test]
fn application_session_keeps_its_two_pointer_layout() {
    use std::mem::size_of;

    use dope_runtime::executor;
    assert_eq!(
        size_of::<executor::session::Application<'static, 'static, ()>>(),
        2 * size_of::<usize>()
    );
}

#[test]
fn application_root_proofs_add_no_storage() {
    use std::mem::size_of;

    use dope_runtime::executor::raw;
    assert_eq!(size_of::<raw::Pending<'static, 'static, ()>>(), 0);
    assert_eq!(
        size_of::<raw::ShutdownRoot<'static, 'static, 'static, ()>>(),
        size_of::<dope_core::driver::Context<'static, 'static>>()
    );
}

#[test]
fn branded_hash_state_has_only_its_two_key_words() {
    use std::mem::size_of;

    assert_eq!(
        size_of::<dope_runtime::random::HashState<'static>>(),
        2 * size_of::<u64>()
    );
    assert_eq!(size_of::<dope_runtime::random::Domain>(), size_of::<u64>());
    assert_eq!(
        size_of::<dope_runtime::random::Hasher<'static>>(),
        size_of::<siphasher::sip::SipHasher13>()
    );
}

#[test]
fn wake_authorities_add_no_descriptor_storage() {
    use std::{mem::size_of, os::fd::OwnedFd};

    let fd = size_of::<OwnedFd>();
    assert_eq!(size_of::<dope_runtime::process::Control>(), fd);
    assert_eq!(size_of::<dope_runtime::process::Shutdown>(), 2 * fd);
    assert_eq!(size_of::<dope_runtime::shutdown::Trigger>(), fd);
    assert_eq!(size_of::<dope_runtime::shutdown::Source>(), 2 * fd);
    assert_eq!(size_of::<dope_runtime::shutdown::Pair>(), 3 * fd);
}

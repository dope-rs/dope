pub mod common;

use common::random::Purpose;
use dope_runtime::{
    executor::{Executor, session},
    process,
    process::gate,
    random, shutdown,
};

trait SendProof<A> {
    const CHECK: ();
}
impl<T: ?Sized> SendProof<()> for T {
    const CHECK: () = ();
}
impl<T: ?Sized + Send> SendProof<u8> for T {
    const CHECK: () = ();
}

trait SyncProof<A> {
    const CHECK: ();
}
impl<T: ?Sized> SyncProof<()> for T {
    const CHECK: () = ();
}
impl<T: ?Sized + Sync> SyncProof<u8> for T {
    const CHECK: () = ();
}

fn send<T: Send>() {}

#[test]
fn thread_traits() {
    let () = <Executor as SendProof<_>>::CHECK;
    let () = <Executor as SyncProof<_>>::CHECK;
    let () = <session::Scope<'static, 'static, 'static, 'static> as SendProof<_>>::CHECK;
    let () = <session::Scope<'static, 'static, 'static, 'static> as SyncProof<_>>::CHECK;
    let () = <process::Context<'static> as SendProof<_>>::CHECK;
    let () = <process::Context<'static> as SyncProof<_>>::CHECK;
    send::<process::Runtime<()>>();
    let () = <process::Shutdown<'static> as SendProof<_>>::CHECK;
    let () = <process::Shutdown<'static> as SyncProof<_>>::CHECK;
    let () = <gate::Closed<'static> as SendProof<_>>::CHECK;
    let () = <gate::Closed<'static> as SyncProof<_>>::CHECK;
    let () = <gate::Open<'static> as SendProof<_>>::CHECK;
    let () = <gate::Open<'static> as SyncProof<_>>::CHECK;
    send::<shutdown::Pair>();
    send::<shutdown::Source>();
    send::<shutdown::ReadySource>();
    send::<shutdown::Readiness>();
    send::<shutdown::Trigger>();
    let () = <shutdown::Requested as SendProof<_>>::CHECK;
    let () = <shutdown::Requested as SyncProof<_>>::CHECK;
    let () = <random::HashState<'static> as SendProof<_>>::CHECK;
    let () = <random::HashState<'static> as SyncProof<_>>::CHECK;
    let () = <random::HashSeed as SendProof<_>>::CHECK;
    let () = <random::HashSeed as SyncProof<_>>::CHECK;
    let () = <random::OwnedHasher as SendProof<_>>::CHECK;
    let () = <random::OwnedHasher as SyncProof<_>>::CHECK;
    let () = <random::Hasher<'static> as SendProof<_>>::CHECK;
    let () = <random::Hasher<'static> as SyncProof<_>>::CHECK;
    let () = <random::Root<'static, Purpose> as SendProof<_>>::CHECK;
    let () = <random::Root<'static, Purpose> as SyncProof<_>>::CHECK;
}

#[test]
fn session_is_two_pointers() {
    use std::mem::size_of;

    use common::runtime::executor::shutdown::Storm;
    assert_eq!(
        size_of::<session::App<'static, 'static, 'static, Storm>>(),
        2 * size_of::<usize>()
    );
}

#[test]
fn root_is_two_words() {
    use std::mem::size_of;

    assert_eq!(
        size_of::<random::Root<'static, Purpose>>(),
        2 * size_of::<u64>()
    );
}

#[test]
fn wakes_add_no_storage() {
    use std::{mem::size_of, os::fd::OwnedFd};

    let word = size_of::<usize>();
    let fd = size_of::<OwnedFd>();
    assert_eq!(size_of::<process::Shutdown<'static>>(), word);
    assert_eq!(size_of::<gate::Closed<'static>>(), word);
    assert_eq!(size_of::<gate::Open<'static>>(), word);
    assert_eq!(size_of::<shutdown::Trigger>(), fd);
    assert_eq!(size_of::<shutdown::Source>(), 2 * fd);
    assert_eq!(size_of::<shutdown::Pair>(), 3 * fd);
    assert_eq!(size_of::<shutdown::ReadySource>(), 3 * fd);
    assert_eq!(size_of::<shutdown::Readiness>(), fd);
    assert_eq!(size_of::<shutdown::ReadyPair>(), 5 * fd);
}

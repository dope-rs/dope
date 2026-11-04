pub mod common;

use common::random::Purpose;
use dope_runtime::{
    executor::{Executor, session},
    process,
    process::gate,
    random, shutdown,
};

trait Ambiguous<A> {
    const CHECK: () = ();
}

impl<T: ?Sized> Ambiguous<()> for T {}
impl<T: ?Sized + Send> Ambiguous<u8> for T {}
impl<T: ?Sized + Sync> Ambiguous<u16> for T {}

fn send<T: Send>() {}

#[test]
fn thread_traits() {
    let () = <Executor as Ambiguous<_>>::CHECK;
    let () = <session::Scope<'static, 'static, 'static, 'static> as Ambiguous<_>>::CHECK;
    let () = <process::Context<'static> as Ambiguous<_>>::CHECK;
    send::<process::Runtime<()>>();
    let () = <process::Shutdown<'static> as Ambiguous<_>>::CHECK;
    let () = <gate::Closed<'static> as Ambiguous<_>>::CHECK;
    let () = <gate::Open<'static> as Ambiguous<_>>::CHECK;
    send::<shutdown::Pair>();
    send::<shutdown::Source>();
    send::<shutdown::ReadySource>();
    send::<shutdown::Readiness>();
    send::<shutdown::Trigger>();
    let () = <shutdown::Requested as Ambiguous<_>>::CHECK;
    let () = <random::HashState<'static> as Ambiguous<_>>::CHECK;
    let () = <random::HashSeed as Ambiguous<_>>::CHECK;
    let () = <random::OwnedHasher as Ambiguous<_>>::CHECK;
    let () = <random::Hasher<'static> as Ambiguous<_>>::CHECK;
    let () = <random::Root<'static, Purpose> as Ambiguous<_>>::CHECK;
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

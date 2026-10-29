use dope_core::{driver::route::KeyTag, io::socket::Inet};
use dope_net::link::oneshot::raw::Io;
use o3::buffer::storage::Shared;

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

#[test]
fn retained_access() {
    type Access = Io<'static, 'static, 'static, 'static, KeyTag<0>, (), Inet, Shared>;
    assert_eq!(
        std::mem::size_of::<Access>(),
        2 * std::mem::size_of::<usize>()
    );
    let () = <Access as SendProof<_>>::CHECK;
    let () = <Access as SyncProof<_>>::CHECK;
}

#[test]
fn prepared_transactions() {
    use dope_net::wire::send::{
        Empty, Plain, Prepared, Transition,
        reclaim::{OnComplete, OnSubmit},
    };

    let () = <Prepared<'static, OnSubmit, Empty> as SendProof<_>>::CHECK;
    let () = <Prepared<'static, OnSubmit, Empty> as SyncProof<_>>::CHECK;
    let () = <Prepared<'static, OnSubmit, Plain<'static>> as SendProof<_>>::CHECK;
    let () = <Prepared<'static, OnSubmit, Plain<'static>> as SyncProof<_>>::CHECK;
    let () = <Prepared<'static, OnComplete, Empty> as SendProof<_>>::CHECK;
    let () = <Prepared<'static, OnComplete, Empty> as SyncProof<_>>::CHECK;
    let () = <Prepared<'static, OnComplete, Plain<'static>> as SendProof<_>>::CHECK;
    let () = <Prepared<'static, OnComplete, Plain<'static>> as SyncProof<_>>::CHECK;
    let () = <Transition<'static, OnSubmit, Empty> as SendProof<_>>::CHECK;
    let () = <Transition<'static, OnSubmit, Empty> as SyncProof<_>>::CHECK;
    let () = <Transition<'static, OnSubmit, Plain<'static>> as SendProof<_>>::CHECK;
    let () = <Transition<'static, OnSubmit, Plain<'static>> as SyncProof<_>>::CHECK;
    let () = <Transition<'static, OnComplete, Empty> as SendProof<_>>::CHECK;
    let () = <Transition<'static, OnComplete, Empty> as SyncProof<_>>::CHECK;
    let () = <Transition<'static, OnComplete, Plain<'static>> as SendProof<_>>::CHECK;
    let () = <Transition<'static, OnComplete, Plain<'static>> as SyncProof<_>>::CHECK;
}

#[test]
fn terminal_permissions() {
    use dope_net::wire::{self, terminal};

    let () = <terminal::Drain<'static, 'static, wire::Identity> as SendProof<_>>::CHECK;
    let () = <terminal::Drain<'static, 'static, wire::Identity> as SyncProof<_>>::CHECK;
    let () = <terminal::Send<'static, ()> as SendProof<_>>::CHECK;
    let () = <terminal::Send<'static, ()> as SyncProof<_>>::CHECK;
    let () = <terminal::Both<'static, ()> as SendProof<_>>::CHECK;
    let () = <terminal::Both<'static, ()> as SyncProof<_>>::CHECK;
}

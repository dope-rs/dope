use dope_core::{driver::route::KeyTag, io::socket::Inet};
use dope_net::link::oneshot::raw::Io;
use o3::buffer::storage::Shared;

trait Ambiguous<A> {
    const CHECK: () = ();
}

impl<T: ?Sized> Ambiguous<()> for T {}
impl<T: ?Sized + Send> Ambiguous<u8> for T {}
impl<T: ?Sized + Sync> Ambiguous<u16> for T {}

#[test]
fn retained_access() {
    type Access = Io<'static, 'static, 'static, 'static, KeyTag<0>, (), Inet, Shared>;
    assert_eq!(
        std::mem::size_of::<Access>(),
        2 * std::mem::size_of::<usize>()
    );
    let () = <Access as Ambiguous<_>>::CHECK;
}

#[test]
fn prepared_transactions() {
    use dope_net::wire::send::{
        Empty, Plain, Prepared, Transition,
        reclaim::{OnComplete, OnSubmit},
    };

    let () = <Prepared<'static, OnSubmit, Empty> as Ambiguous<_>>::CHECK;
    let () = <Prepared<'static, OnSubmit, Plain<'static>> as Ambiguous<_>>::CHECK;
    let () = <Prepared<'static, OnComplete, Empty> as Ambiguous<_>>::CHECK;
    let () = <Prepared<'static, OnComplete, Plain<'static>> as Ambiguous<_>>::CHECK;
    let () = <Transition<'static, OnSubmit, Empty> as Ambiguous<_>>::CHECK;
    let () = <Transition<'static, OnSubmit, Plain<'static>> as Ambiguous<_>>::CHECK;
    let () = <Transition<'static, OnComplete, Empty> as Ambiguous<_>>::CHECK;
    let () = <Transition<'static, OnComplete, Plain<'static>> as Ambiguous<_>>::CHECK;
}

#[test]
fn terminal_permissions() {
    use dope_net::wire::{self, terminal};

    let () = <terminal::Drain<'static, 'static, wire::Identity> as Ambiguous<_>>::CHECK;
    let () = <terminal::Send<'static, ()> as Ambiguous<_>>::CHECK;
    let () = <terminal::Both<'static, ()> as Ambiguous<_>>::CHECK;
}

use std::os::fd::OwnedFd;

use dope_core::io::fd::outbound;
pub mod common;

use dope_core::{
    driver,
    driver::{
        life,
        life::finish,
        receive::{self, credit},
        retained::raw,
        route,
        route::{
            KeyTag,
            lease::{Claim, Owner},
        },
        schedule::{
            app::{self, step},
            clock,
            clock::timer::{Alarm, Timer},
            maint, ready,
            ready::{task, wake, wake::store},
            state::scope,
        },
    },
    io,
    io::{datagram, fd::handles, fs, recv, socket::Listener},
    platform::{
        channel::Channel,
        poll,
        wake::{Notify, Wait},
    },
};

use crate::common::driver::task::Index;

trait MaybeSend<A> {}
impl<T: ?Sized> MaybeSend<()> for T {}
impl<T: ?Sized + Send> MaybeSend<u8> for T {}

trait MaybeSync<A> {}
impl<T: ?Sized> MaybeSync<()> for T {}
impl<T: ?Sized + Sync> MaybeSync<u8> for T {}

fn confined<T: ?Sized + MaybeSend<A> + MaybeSync<B>, A, B>() {}

const _: fn() = || {
    fn exclusive<T: ?Sized + MaybeSync<A>, A>() {}
    exclusive::<Channel, _>();
    exclusive::<Wait, _>();
    exclusive::<Notify, _>();
    exclusive::<datagram::Group, _>();
    exclusive::<datagram::Lease, _>();
    exclusive::<Listener, _>();
};

const _: fn() = || {
    use raw::Submission;

    confined::<driver::Driver, _, _>();
    confined::<driver::Context<'static, 'static>, _, _>();
    confined::<driver::Reference<'static>, _, _>();
    confined::<receive::Attempt<'static>, _, _>();
    confined::<clock::Clock<'static, 'static>, _, _>();
    confined::<clock::Moment<'static>, _, _>();
    confined::<clock::Deadline<'static>, _, _>();
    confined::<scope::Scope<'static, 'static>, _, _>();
    confined::<scope::Guard<'static, 'static>, _, _>();
    confined::<app::Permit<'static, 'static>, _, _>();
    confined::<maint::Permit<'static, 'static>, _, _>();
    confined::<step::Delegated<'static, 'static, 'static>, _, _>();
    confined::<step::Receipt<'static, 'static>, _, _>();
    confined::<poll::Set<'static>, _, _>();
    confined::<poll::Ready<'static, 'static>, _, _>();
    confined::<Owner<'static, KeyTag<0>>, _, _>();
    confined::<Claim<'static, KeyTag<0>>, _, _>();
    confined::<life::Install<'static, 'static>, _, _>();
    confined::<finish::Finish<'static, 'static>, _, _>();
    confined::<finish::Credit<'static, 'static>, _, _>();
    confined::<io::Event<'static>, _, _>();
    confined::<recv::Lease<'static>, _, _>();
    confined::<credit::Pending<'static, KeyTag<0>>, _, _>();
    confined::<credit::Held<'static, KeyTag<0>>, _, _>();
    confined::<credit::Return<'static>, _, _>();
    confined::<outbound::Reserved<'static, KeyTag<0>>, _, _>();
    confined::<fs::Dir, _, _>();
    confined::<fs::File, _, _>();
    confined::<Alarm<'static>, _, _>();
    confined::<Timer<'static>, _, _>();
    confined::<Submission<'static, 'static, KeyTag<1>>, _, _>();
    confined::<handles::Descriptor<'static>, _, _>();
    confined::<handles::SocketSlot<'static>, _, _>();
    confined::<handles::Creation<'static>, _, _>();
    confined::<handles::CreatedSlot<'static>, _, _>();
    confined::<route::Token, _, _>();
    confined::<ready::Key, _, _>();
    confined::<wake::Waker<'static>, _, _>();
    confined::<store::Store<'static>, _, _>();
    confined::<store::Entry<'static>, _, _>();
    confined::<store::Slot<'static>, _, _>();
};

#[test]
fn keeps_local_layouts() {
    assert_eq!(size_of::<Channel>(), size_of::<OwnedFd>());
    let layout = if datagram::GSO_LIMITS.is_some() {
        (24, 8)
    } else {
        (0, 1)
    };
    assert_eq!(size_of::<datagram::GsoControl>(), layout.0);
    assert_eq!(align_of::<datagram::GsoControl>(), layout.1);
    assert_eq!(size_of::<Option<datagram::GsoControl>>(), layout.0);
    assert_eq!(
        size_of::<handles::Descriptor<'static>>(),
        2 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<ready::Slot<'static, KeyTag<1>>>(),
        2 * size_of::<u64>()
    );
    assert_eq!(
        size_of::<task::raw::Node<'static, Index>>(),
        3 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<task::raw::Binding<'static, 'static, Index>>(),
        4 * size_of::<usize>()
    );
    assert_eq!(size_of::<store::Entry<'static>>(), size_of::<usize>());
    assert_eq!(
        size_of::<finish::Finish<'static, 'static>>(),
        2 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<outbound::Reserved<'static, KeyTag<0>>>(),
        size_of::<usize>()
    );
}

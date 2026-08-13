use dope_net::link::pool::pending;
use dope_test::checks::affinities::Affinity;

const _: fn() = || {
    Affinity::<dope_net::link::pool::Key<'static, 0>>::not_send::<_>();
    Affinity::<dope_net::link::pool::Key<'static, 0>>::not_sync::<_>();
    Affinity::<dope_net::wire::RecvCredit<'static, 0>>::not_send::<_>();
    Affinity::<dope_net::wire::RecvCredit<'static, 0>>::not_sync::<_>();
    Affinity::<dope_net::wire::RecvCreditGuard<'static, 0>>::not_send::<_>();
    Affinity::<dope_net::wire::RecvCreditGuard<'static, 0>>::not_sync::<_>();
    Affinity::<pending::Handle<'static>>::not_send::<_>();
    Affinity::<pending::Handle<'static>>::not_sync::<_>();
};

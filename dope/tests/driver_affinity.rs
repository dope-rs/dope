trait AmbiguousIfSend<A> {}

impl<T: ?Sized> AmbiguousIfSend<()> for T {}

impl<T: ?Sized + Send> AmbiguousIfSend<u8> for T {}

trait AmbiguousIfSync<A> {}

impl<T: ?Sized> AmbiguousIfSync<()> for T {}

impl<T: ?Sized + Sync> AmbiguousIfSync<u8> for T {}

const _: fn() = || {
    fn not_send<T: ?Sized + AmbiguousIfSend<A>, A>() {}
    fn not_sync<T: ?Sized + AmbiguousIfSync<A>, A>() {}
    fn require_send<T: Send>() {}

    not_send::<dope::driver::Driver, _>();
    not_sync::<dope::driver::Driver, _>();
    not_send::<dope::runtime::executor::Executor, _>();
    not_sync::<dope::runtime::executor::Executor, _>();
    not_send::<dope::runtime::executor::Session<'static, 'static>, _>();
    not_sync::<dope::runtime::executor::Session<'static, 'static>, _>();
    not_send::<dope::runtime::launcher::WorkerContext, _>();
    not_sync::<dope::runtime::launcher::WorkerContext, _>();
    not_send::<dope::hash::State, _>();
    not_sync::<dope::hash::State, _>();
    not_sync::<dope::runtime::launcher::Launcher, _>();
    require_send::<dope::runtime::launcher::Launcher>();
    not_sync::<dope::runtime::trigger::ShutdownTrigger, _>();
    require_send::<dope::runtime::trigger::ShutdownTrigger>();
    not_send::<dope::runtime::trigger::SignalShutdown, _>();
    not_sync::<dope::runtime::trigger::SignalShutdown, _>();
    not_send::<dope::driver::OutboundReservation, _>();
    not_sync::<dope::driver::OutboundReservation, _>();
    not_send::<dope::io::file::OsFile, _>();
    not_sync::<dope::io::file::OsFile, _>();
    not_send::<dope::manifold::timer::Ticket, _>();
    not_sync::<dope::manifold::timer::Ticket, _>();
    not_send::<dope::manifold::timer::Timer<'static>, _>();
    not_sync::<dope::manifold::timer::Timer<'static>, _>();
    not_send::<dope::manifold::connector::source::DialKey, _>();
    not_sync::<dope::manifold::connector::source::DialKey, _>();
};

#[test]
fn local_capabilities_keep_their_layout() {
    assert_eq!(
        size_of::<dope::manifold::timer::Ticket>(),
        2 * size_of::<u32>()
    );
    assert_eq!(
        size_of::<dope::manifold::connector::source::DialKey>(),
        2 * size_of::<u32>()
    );
    assert_eq!(
        size_of::<dope::driver::OutboundReservation>(),
        2 * size_of::<u32>()
    );
}

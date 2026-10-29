use std::convert::Infallible;

use dope_test::harness::scenario::TIMEOUT;
use engine::{
    core::{
        driver::life::{
            creation::Attempt,
            recovery::{Policy, Recovery},
        },
        io::Reclaimed,
    },
    runtime::executor::session::Completion,
};

use crate::common::native;
pub mod common;

use std::{
    cell::Cell,
    marker::{PhantomData, PhantomPinned},
    rc::Rc,
};

use counter::Counter;
use engine::{
    core::driver::{route, route::indices::SlotIndex, settings::Routes},
    manifold::app::dispatch,
    runtime::executor,
};
use fibers::{BlockOn as _, abi::Ready};

use crate::common::macros::{fairness::tree::Branch, *};

#[test]
fn path_modes() {
    fn require<T: executor::raw::App<'static>>() {}

    require::<Facade>();
    require::<Parts>();
}

#[test]
fn forward_layout() {
    assert!((<Forward as dispatch::raw::Manifold<'static>>::ID) == (route::Id::fixed::<6>()));
    assert!(
        <Defaulted<'static> as dispatch::raw::Manifold<'static>>::ID == route::Id::fixed::<9>()
    );
}

#[test]
fn retained_lifetime() {
    fn assert_forward<'d>()
    where
        Retained<'d>: dispatch::raw::Manifold<'d>,
    {
    }
    fn assert_dispatcher<'d>()
    where
        Lifetime<'d>: executor::raw::App<'d>,
    {
    }

    assert_forward::<'static>();
    assert_dispatcher::<'static>();
}

#[test]
fn route_consts() {
    assert!((Dispatcher::A_ROUTE) == (route::Id::fixed::<3>()));
    assert!((Dispatcher::B_ROUTE) == (route::Id::fixed::<0>()));
    assert!((Dispatcher::C_ROUTE) == (route::Id::fixed::<1>()));
    assert!(
        <Dispatcher as executor::raw::App<'static>>::ROUTES
            == Routes::singleton_id(route::Id::fixed::<3>())
                .join(Routes::singleton_id(route::Id::fixed::<0>()))
                .join(Routes::singleton_id(route::Id::fixed::<1>()))
    );
}

#[test]
fn explicit_lifetime() {
    assert!((Branded::INNER_ROUTE) == (route::Id::fixed::<5>()));
}

#[test]
fn selected_lifetime() {
    fn assert_lifetime<'scope, 'd>(
        app: &Selected<'scope, 'd>,
        brand: PhantomData<fn(&'d ()) -> &'d ()>,
    ) {
        fn assert_application<'d>(
            _: &impl executor::raw::App<'d>,
            _: PhantomData<fn(&'d ()) -> &'d ()>,
        ) {
        }
        assert_application(app, brand);
    }

    let app = Selected {
        inner: Counter::default(),
    };
    assert_lifetime(&app, PhantomData);
    assert!((Selected::<'_, '_>::INNER_ROUTE) == (route::Id::fixed::<8>()));
}

#[test]
fn state_routing() {
    assert!((Stateful::INNER_ROUTE) == (route::Id::fixed::<4>()));
}

#[test]
fn forward_lifetime() {
    fn assert_manifold<'d>(_: &impl dispatch::raw::Manifold<'d>) {}

    let app = Scoped {
        inner: Counter::default(),
    };
    assert_manifold(&app);
    assert!(
        <Scoped<'static, 'static> as dispatch::raw::Manifold<'static>>::ID
            == route::Id::fixed::<7>()
    );
}

#[test]
fn block_ticks_every_field() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    executor.ok().expect("created executor").run(|executor| {
        let result = executor.enter_with(
            |mut sess| {
                let app = Dispatcher::default();
                let ticks = [
                    Rc::clone(&app.a.calls.tick),
                    Rc::clone(&app.b.calls.tick),
                    Rc::clone(&app.c.calls.tick),
                ];
                let result = sess.with_quiesce(&mut report, app, TIMEOUT, |mut app| {
                    app.block_on(Ready::new(()))
                        .expect("dispatcher fiber stalled");
                });
                assert!(result.is_ok(), "application teardown");
                assert!(ticks.into_iter().all(|ticks| ticks.get() >= 1));
            },
            Policy::new(shutdown, recover),
        );
        assert!(result.is_ok(), "test runtime completed");
    });
}

#[test]
fn lifecycle_counts() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    executor.ok().expect("created executor").run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let app = Dispatcher::default();
                let shutdown = [
                    Rc::clone(&app.a.calls.shutdown),
                    Rc::clone(&app.b.calls.shutdown),
                    Rc::clone(&app.c.calls.shutdown),
                ];
                let install = [
                    Rc::clone(&app.a.calls.install),
                    Rc::clone(&app.b.calls.install),
                    Rc::clone(&app.c.calls.install),
                ];
                let finish = [
                    Rc::clone(&app.a.calls.finish),
                    Rc::clone(&app.b.calls.finish),
                    Rc::clone(&app.c.calls.finish),
                ];
                let result = session.with_quiesce(&mut report, app, TIMEOUT, |_app| {});
                assert!(result.is_ok(), "dispatcher teardown");
                assert!(install.into_iter().all(|calls| calls.get() == 1));
                assert!(shutdown.into_iter().all(|calls| calls.get() == 1));
                assert!(finish.into_iter().all(|calls| calls.get() == 1));

                let forward = Counter::default();
                let install = Rc::clone(&forward.calls.install);
                let shutdown = Rc::clone(&forward.calls.shutdown);
                let finish = Rc::clone(&forward.calls.finish);
                let result = session.with_quiesce(
                    &mut report,
                    Nested {
                        inner: Forward { inner: forward },
                    },
                    TIMEOUT,
                    |_app| {},
                );
                assert!(result.is_ok(), "forward teardown");
                assert_eq!(install.get(), 1);
                assert_eq!(shutdown.get(), 1);
                assert_eq!(finish.get(), 1);
            },
            Policy::new(shutdown, recover),
        );
        assert!(result.is_ok(), "test runtime completed");
    });
}

#[test]
fn shared_budgets() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    executor.ok().expect("created executor").run(|executor| {
        let result = executor.enter_with(
            |mut sess| {
                let first = [
                    Rc::new(Cell::new(None)),
                    Rc::new(Cell::new(None)),
                    Rc::new(Cell::new(None)),
                ];
                let app = Metered {
                    a: Budget {
                        first: Rc::clone(&first[0]),
                        marker: PhantomData,
                        _pinned: PhantomPinned,
                    },
                    b: Budget {
                        first: Rc::clone(&first[1]),
                        marker: PhantomData,
                        _pinned: PhantomPinned,
                    },
                    c: Budget {
                        first: Rc::clone(&first[2]),
                        marker: PhantomData,
                        _pinned: PhantomPinned,
                    },
                };
                let result = sess.with_quiesce(&mut report, app, TIMEOUT, |mut app| {
                    app.block_on(Ready::new(()))
                        .expect("priority fiber stalled");
                });
                assert!(result.is_ok(), "application teardown");
                assert_eq!(
                    first.map(|value| value.get()),
                    [Some((86, 85)), Some((85, 85)), Some((85, 85))]
                );
            },
            Policy::new(shutdown, recover),
        );
        assert!(result.is_ok(), "test runtime completed");
    });
}

#[test]
fn routes_active_owners() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    executor.ok().expect("created executor").run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let driver = session.driver().driver_ref();
                let target = driver
                    .targets::<route::KeyTag<3>>()
                    .bind(SlotIndex::ZERO, route::Epoch::INITIAL)
                    .dispatch();
                let absent = driver.ready().make_slot(target).expect("optional slot");
                let target = driver
                    .targets::<route::KeyTag<0>>()
                    .bind(SlotIndex::ZERO, route::Epoch::INITIAL)
                    .dispatch();
                let ready = driver.ready().make_slot(target).expect("nested slot");
                let nested = Dispatcher::default();
                let calls = [
                    Rc::clone(&nested.a.calls.activate),
                    Rc::clone(&nested.b.calls.activate),
                    Rc::clone(&nested.c.calls.activate),
                ];
                absent.activate();
                ready.activate();
                session
                    .with_quiesce(
                        &mut report,
                        Branch {
                            first: Optional { optional: None },
                            second: nested,
                        },
                        TIMEOUT,
                        |mut app| app.block_on(Ready::new(())).expect("wake turn"),
                    )
                    .expect("application teardown");
                assert_eq!(calls.map(|count| count.get()), [1, 1, 0]);

                let duplicate = Counter::default();
                let lifecycle = [
                    Rc::clone(&duplicate.calls.install),
                    Rc::clone(&duplicate.calls.shutdown),
                    Rc::clone(&duplicate.calls.finish),
                    Rc::clone(&duplicate.calls.tick),
                ];
                let rejected: executor::session::Result<'_, ()> = session.with_quiesce(
                    &mut report,
                    Branch {
                        first: Optional {
                            optional: Some(duplicate),
                        },
                        second: Dispatcher::default(),
                    },
                    TIMEOUT,
                    |_app| panic!("duplicate owner admitted"),
                );
                assert!(matches!(
                    rejected,
                    Err(executor::session::Error::Install {
                        cause: executor::InstallError::Claim(
                            engine::core::driver::settings::errors::Claim::DuplicateRoute(id)
                        ),
                        disarm: None,
                    }) if id == route::Id::fixed::<3>()
                ));
                assert_eq!(lifecycle.map(|count| count.get()), [1, 1, 1, 0]);
            },
            Policy::new(shutdown, recover),
        );
        assert!(result.is_ok(), "test runtime completed");
    });
}

fn shutdown(event: Reclaimed) {
    assert!(
        matches!(event, Reclaimed::Shutdown),
        "unexpected operation: {event:?}"
    );
}

fn recover(failure: Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}

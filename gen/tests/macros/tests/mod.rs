use std::{
    cell::Cell,
    marker::{PhantomData, PhantomPinned},
    rc::Rc,
};

use dope::manifold::dispatch::raw::Manifold;
use dope_test::{fibers, scenario::rt::Runtime};
use fiber_rt::abi::{Pending, Ready};

mod sealed;

struct Counter<const ID: u8, M = ()> {
    dispatch_calls: Cell<u32>,
    activate_calls: Cell<u32>,
    tick_calls: Rc<Cell<u32>>,
    idle_calls: Cell<u32>,
    install_calls: Rc<Cell<u32>>,
    shutdown_calls: Rc<Cell<u32>>,
    finish_calls: Rc<Cell<u32>>,
    pending: bool,
    marker: PhantomData<fn() -> M>,
    _pinned: PhantomPinned,
}

impl<const ID: u8, M> Counter<ID, M> {
    fn new(pending: bool) -> Self {
        Self {
            dispatch_calls: Cell::new(0),
            activate_calls: Cell::new(0),
            tick_calls: Rc::new(Cell::new(0)),
            idle_calls: Cell::new(0),
            install_calls: Rc::new(Cell::new(0)),
            shutdown_calls: Rc::new(Cell::new(0)),
            finish_calls: Rc::new(Cell::new(0)),
            pending,
            marker: PhantomData,
            _pinned: PhantomPinned,
        }
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct Dispatcher {
    #[pin]
    #[manifold]
    a: Counter<3>,
    #[pin]
    #[manifold]
    b: Counter<0>,
    #[pin]
    #[manifold]
    c: Counter<1>,
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct BrandedDispatcher<'d> {
    #[pin]
    #[manifold]
    inner: Counter<5>,
    #[dispatcher(marker)]
    brand: ::core::marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct NonleadingBrandedDispatcher<'scope, 'd> {
    #[pin]
    #[manifold]
    inner: Counter<8, (&'scope (), &'d ())>,
    #[dispatcher(marker)]
    brand: ::core::marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct StatefulDispatcher {
    #[pin]
    #[manifold]
    inner: Counter<4>,
    #[dispatcher(state)]
    state: usize,
}

struct ScheduledState;

impl dope::manifold::timing::Schedule for ScheduledState {
    fn deadline(&self) -> Option<std::time::Instant> {
        None
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct ScheduledDispatcher {
    #[dispatcher(schedule)]
    state: ScheduledState,
}

struct BudgetCounter<const ID: u8> {
    first: Rc<Cell<Option<usize>>>,
    _pinned: PhantomPinned,
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct BudgetDispatcher {
    #[pin]
    #[manifold]
    a: BudgetCounter<3>,
    #[pin]
    #[manifold]
    b: BudgetCounter<0>,
    #[pin]
    #[manifold]
    c: BudgetCounter<1>,
}

#[pin_project::pin_project]
#[derive(dope_gen::Forward)]
struct ScopedForward<'scope, 'd> {
    #[pin]
    #[forward('d)]
    inner: Counter<7, (&'scope (), &'d ())>,
}

#[pin_project::pin_project]
#[derive(dope_gen::Forward)]
struct PinnedForward {
    #[pin]
    #[forward]
    inner: Counter<6>,
}

#[pin_project::pin_project]
#[derive(dope_gen::Forward)]
struct LifetimeForward<'d> {
    #[pin]
    #[forward]
    inner: dope::manifold::timing::interval::Interval<'d, 10>,
}

#[pin_project::pin_project]
#[derive(dope_gen::Forward)]
struct Capability<'d> {
    #[pin]
    #[forward('d, capability = dope::manifold::dispatch::raw::Plain)]
    inner: dope::manifold::timing::interval::Interval<'d, 13>,
}

#[pin_project::pin_project]
#[derive(dope_gen::Forward)]
struct Defaulted<'d, M = (), const ID: u8 = 9> {
    #[pin]
    #[forward('d)]
    inner: Counter<ID, (&'d (), M)>,
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct LifetimeDispatcher<'d> {
    #[pin]
    #[manifold]
    inner: dope::manifold::timing::interval::Interval<'d, 11>,
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct ForwardDispatcher {
    #[pin]
    #[manifold]
    inner: PinnedForward,
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct BorrowedDispatcher<'app, 'd> {
    #[pin]
    #[manifold(borrowed)]
    inner: dope::runtime::client::Anchor<'app, Counter<12>>,
    #[dispatcher(marker)]
    brand: ::core::marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

fn make_dispatcher() -> Dispatcher {
    Dispatcher {
        a: Counter::new(false),
        b: Counter::new(false),
        c: Counter::new(false),
    }
}

#[test]
fn forward_structurally_projects() {
    assert_eq!(PinnedForward::ID, 6);
    let _: Option<Defaulted<'static>> = None;
    assert_eq!(Defaulted::<'static, (), 9>::ID, 9);
    assert_eq!(
        std::mem::size_of::<PinnedForward>(),
        std::mem::size_of::<Counter<6>>()
    );
}

#[test]
fn real_retained_owner_preserves_driver_lifetime_without_layout_cost() {
    fn assert_forward<'d>()
    where
        LifetimeForward<'d>: Manifold<'d>,
    {
    }
    fn assert_capability_forward<'d>()
    where
        Capability<'d>: Manifold<'d>,
    {
    }
    fn assert_dispatcher<'d>()
    where
        LifetimeDispatcher<'d>: dope::runtime::executor::Application<'d>,
    {
    }

    assert_forward::<'static>();
    assert_capability_forward::<'static>();
    assert_dispatcher::<'static>();
    assert_eq!(
        std::mem::size_of::<LifetimeForward<'static>>(),
        std::mem::size_of::<dope::manifold::timing::interval::Interval<'static, 10>>(),
    );
    assert_eq!(
        std::mem::size_of::<LifetimeDispatcher<'static>>(),
        std::mem::size_of::<dope::manifold::timing::interval::Interval<'static, 11>>(),
    );
    assert_eq!(
        std::mem::size_of::<Capability<'static>>(),
        std::mem::size_of::<dope::manifold::timing::interval::Interval<'static, 13>>(),
    );
}

#[test]
fn borrowed_owner_is_dispatched_without_a_wrapper_manifold() {
    fn assert_dispatcher<'app, 'd>()
    where
        BorrowedDispatcher<'app, 'd>: dope::runtime::executor::Application<'d>,
    {
    }

    assert_dispatcher::<'static, 'static>();
    assert_eq!(
        std::mem::size_of::<BorrowedDispatcher<'static, 'static>>(),
        std::mem::size_of::<usize>(),
    );
}

#[dope_gen::fiber_fn('d, crate = ::fiber_rt)]
async fn sum_repeated<'d>() -> usize {
    let mut sum = 0usize;
    for value in 1usize..=4 {
        sum += Ready::new(value).await;
    }
    sum
}

#[dope_gen::fiber_fn('d, crate = ::fiber_rt)]
async fn wait_repeated<'d>() -> usize {
    loop {
        Pending::<()>::new().await;
    }
}

fn assert_usize_output<'d>(_: &impl fiber_rt::abi::Fiber<'d, Output = usize>) {}

#[test]
fn route_consts() {
    assert_eq!(Dispatcher::A_ROUTE, 3);
    assert_eq!(Dispatcher::B_ROUTE, 0);
    assert_eq!(Dispatcher::C_ROUTE, 1);
}

#[test]
fn dispatcher_accepts_an_explicit_lifetime_marker() {
    let dispatcher = BrandedDispatcher {
        inner: Counter::new(false),
        brand: PhantomData,
    };
    assert_eq!(BrandedDispatcher::INNER_ROUTE, 5);
    assert_eq!(
        std::mem::size_of_val(&dispatcher),
        std::mem::size_of::<Counter<5>>()
    );
}

#[test]
fn dispatcher_uses_the_marker_lifetime_instead_of_the_first_lifetime() {
    fn assert_exact_driver_lifetime<'scope, 'd>(
        dispatcher: &NonleadingBrandedDispatcher<'scope, 'd>,
        brand: PhantomData<fn(&'d ()) -> &'d ()>,
    ) {
        fn assert_application<'d>(
            _: &impl dope::runtime::executor::Application<'d>,
            _: PhantomData<fn(&'d ()) -> &'d ()>,
        ) {
        }
        assert_application(dispatcher, brand);
    }

    let dispatcher = NonleadingBrandedDispatcher {
        inner: Counter::new(false),
        brand: PhantomData,
    };
    assert_exact_driver_lifetime(&dispatcher, PhantomData);
    assert_eq!(NonleadingBrandedDispatcher::<'_, '_>::INNER_ROUTE, 8);
}

#[test]
fn dispatcher_keeps_non_lifecycle_state_out_of_routing() {
    let dispatcher = StatefulDispatcher {
        inner: Counter::new(false),
        state: 17,
    };
    assert_eq!(StatefulDispatcher::INNER_ROUTE, 4);
    assert_eq!(dispatcher.state, 17);
    assert_eq!(
        std::mem::size_of_val(&dispatcher),
        std::mem::size_of::<Counter<4>>() + std::mem::size_of::<usize>()
    );
}

#[test]
fn forward_can_name_nonleading_driver_lifetime() {
    fn assert_manifold<'d>(_: &impl Manifold<'d>) {}

    let app = ScopedForward {
        inner: Counter::new(false),
    };
    assert_manifold(&app);
    assert_eq!(ScopedForward::<'_, '_>::ID, 7);
}

#[test]
fn block_ticks_every_field() {
    Runtime::throughput().with_session(|mut sess| {
        let dispatcher = make_dispatcher();
        let ticks = [
            Rc::clone(&dispatcher.a.tick_calls),
            Rc::clone(&dispatcher.b.tick_calls),
            Rc::clone(&dispatcher.c.tick_calls),
        ];
        sess.with_app(dispatcher, |mut app| {
            fibers::TEST.drive(&mut app, Ready::new(()));
        })
        .expect("application teardown");
        assert!(ticks.into_iter().all(|ticks| ticks.get() >= 1));
    });
}

#[test]
fn lifecycle_context_visits_each_field_and_forward_exactly_once() {
    Runtime::throughput().with_session(|mut session| {
        let dispatcher = make_dispatcher();
        let shutdown = [
            Rc::clone(&dispatcher.a.shutdown_calls),
            Rc::clone(&dispatcher.b.shutdown_calls),
            Rc::clone(&dispatcher.c.shutdown_calls),
        ];
        let install = [
            Rc::clone(&dispatcher.a.install_calls),
            Rc::clone(&dispatcher.b.install_calls),
            Rc::clone(&dispatcher.c.install_calls),
        ];
        let finish = [
            Rc::clone(&dispatcher.a.finish_calls),
            Rc::clone(&dispatcher.b.finish_calls),
            Rc::clone(&dispatcher.c.finish_calls),
        ];
        session
            .with_app(dispatcher, |_app| {})
            .expect("dispatcher teardown");
        assert!(install.into_iter().all(|calls| calls.get() == 1));
        assert!(shutdown.into_iter().all(|calls| calls.get() == 1));
        assert!(finish.into_iter().all(|calls| calls.get() == 1));

        let forwarded = Counter::new(false);
        let install = Rc::clone(&forwarded.install_calls);
        let shutdown = Rc::clone(&forwarded.shutdown_calls);
        let finish = Rc::clone(&forwarded.finish_calls);
        session
            .with_app(
                ForwardDispatcher {
                    inner: PinnedForward { inner: forwarded },
                },
                |_app| {},
            )
            .expect("forward teardown");
        assert_eq!(install.get(), 1);
        assert_eq!(shutdown.get(), 1);
        assert_eq!(finish.get(), 1);
    });
}

#[test]
fn maintenance_budget_is_shared_across_fields() {
    Runtime::throughput().with_session(|mut sess| {
        let first = [
            Rc::new(Cell::new(None)),
            Rc::new(Cell::new(None)),
            Rc::new(Cell::new(None)),
        ];
        let dispatcher = BudgetDispatcher {
            a: BudgetCounter {
                first: Rc::clone(&first[0]),
                _pinned: PhantomPinned,
            },
            b: BudgetCounter {
                first: Rc::clone(&first[1]),
                _pinned: PhantomPinned,
            },
            c: BudgetCounter {
                first: Rc::clone(&first[2]),
                _pinned: PhantomPinned,
            },
        };
        sess.with_app(dispatcher, |mut app| {
            fibers::TEST.drive(&mut app, Ready::new(()));
        })
        .expect("application teardown");
        assert_eq!(
            first.map(|value| value.get()),
            [Some(86), Some(85), Some(85)]
        );
    });
}

#[test]
fn fiber_fn_runs_repeated_awaits() {
    let pending = wait_repeated();
    assert_usize_output(&pending);
    drop(pending);
    Runtime::throughput().with_session(|mut sess| {
        sess.with_app(make_dispatcher(), |mut app| {
            assert_eq!(fibers::TEST.drive(&mut app, sum_repeated()), 10);
        })
        .expect("application teardown");
    });
}

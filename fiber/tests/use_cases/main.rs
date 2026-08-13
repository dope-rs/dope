#![deny(unsafe_code)]

use std::{cell::Cell, io, mem::size_of, pin::Pin, rc::Rc, task::Poll};

use dope::{
    core::driver::{
        route::{Epoch, FRAMEWORK, KeyTag, SlotIndex, Token},
        schedule::{ready::completion, timer::Registration},
        settings::{Profile, QueueLayout, SchedulerLayout},
    },
    manifold::timing::{Policy, Window},
    net::{link::pool::pending::Work, wire::Identity},
    runtime::shutdown,
};
use dope_fiber::{
    abi::{
        Fiber, Pending, Ready,
        batch::{Batch, Domain},
    },
    context::{PollCall, RootWaker, Waker},
    extensions::AppSessionExt,
    net::Io,
    task::{
        Scheduler,
        sleep::Sleep,
        storage::{Slab, fixed},
    },
    wait::{Queue, Registry, Slot, Table, Waiter},
};
use dope_test::{dispatch, scenario::rt::Runtime};
use o3::collections::{
    fixed::pinned::Slice,
    slab::{Capacity, pinned},
};

struct NoopHooks;

impl<'d> dispatch::Hooks<'d, ()> for NoopHooks {}

#[test]
fn batch_preserves_an_unadmitted_child_at_the_turn_boundary() {
    Runtime::throughput().with_session(|mut session| {
        let completed = session
            .with_app(dispatch::Builder::new(NoopHooks).probe::<0>(), |mut app| {
                app.block_on(dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                    const N: usize = dope::core::driver::schedule::MAX_TURN_WORK_BUDGET;
                    let mut domain = Domain::<N>::acquire()
                        .await
                        .expect("batch ready domain");
                    Batch::<Ready<()>, (), N>::try_from_array(
                        &mut domain,
                        core::array::from_fn(|_| Ready::new(())),
                    )
                    .expect("batch queue allocation")
                    .await
                    .count()
                }))
            })
            .expect("application teardown")
            .expect("batch crossed the application turn boundary");
        assert_eq!(
            completed,
            dope::core::driver::schedule::MAX_TURN_WORK_BUDGET
        );
    });
}

#[test]
fn raw_hot_path_boundaries_add_no_storage() {
    use dope_fiber::{
        abi::future::raw::{Brand, Seal},
        wait::{self, Slots},
    };

    assert_eq!(size_of::<Brand<'static>>(), 0);
    assert_eq!(size_of::<Seal<'static>>(), 0);
    assert_eq!(
        size_of::<
            Io<'static, 'static, Identity, dope::manifold::connector::connection::Id<'static, 0>>,
        >(),
        size_of::<(&'static (), Token)>(),
    );
    assert_eq!(
        size_of::<dope_fiber::net::read::Lease<'static, 'static, Identity>>(),
        size_of::<dope::core::io::recv::View<'static>>(),
    );
    assert_eq!(size_of::<Work>(), size_of::<u8>());
    assert_eq!(size_of::<RootWaker<'static>>(), size_of::<[usize; 2]>());
    assert_eq!(size_of::<Waker<'static>>(), size_of::<[usize; 2]>());
    assert_eq!(
        size_of::<Option<RootWaker<'static>>>(),
        size_of::<[usize; 2]>()
    );
    assert_eq!(size_of::<Option<Waker<'static>>>(), size_of::<[usize; 2]>());
    assert_eq!(
        size_of::<PollCall<'static, 'static, 'static, Ready<()>>>(),
        size_of::<[usize; 2]>()
    );
    assert_eq!(
        size_of::<completion::Waker<'static>>(),
        size_of::<[usize; 2]>()
    );
    assert_eq!(size_of::<Registry<'static>>(), size_of::<[usize; 4]>());
    assert_eq!(size_of::<wait::Slot>(), size_of::<usize>());
    assert_eq!(
        size_of::<Queue<'static>>(),
        size_of::<Pin<Box<Registry<'static>>>>()
    );
    assert_eq!(
        size_of::<Table<'static>>(),
        size_of::<Slice<Registry<'static>>>()
    );
    assert_eq!(size_of::<Slots<'static>>(), size_of::<Slice<wait::Slot>>());
    assert_eq!(
        size_of::<Waiter<'static, 'static>>(),
        size_of::<[usize; 5]>()
    );
    assert_eq!(
        size_of::<Slab<'static, Ready<()>>>(),
        size_of::<pinned::Pool<Ready<()>>>(),
    );
    assert_eq!(
        size_of::<fixed::Slab<'static, Ready<()>, 1>>(),
        size_of::<pinned::Fixed<Ready<()>, 1>>(),
    );
    assert_eq!(
        size_of::<Scheduler<'static, Ready<()>>>(),
        size_of::<Slab<'static, Ready<()>>>() + size_of::<Pin<Box<()>>>(),
    );
    assert_eq!(
        size_of::<Sleep<'static, 'static>>(),
        size_of::<Registration<'static, 'static>>(),
    );
}

#[test]
fn waker_identity_is_the_exact_driver_and_wake_target() {
    Runtime::throughput().with_session(|mut sess| {
        let driver = sess.driver_access().driver_ref();
        let targets = driver.targets::<KeyTag<0>>();
        let first = driver
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("ready slot");
        let second = driver
            .ready()
            .make_ready_slot(
                targets
                    .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                    .dispatch(),
            )
            .expect("ready slot");
        let registered = Cell::new(Some(Waker::from(RootWaker::from(first.target()))));
        let same = Waker::from(RootWaker::from(first.target()));
        let distinct = Waker::from(RootWaker::from(second.target()));

        assert!(registered.get().is_some_and(|current| current == same));
        assert!(registered.get().is_some_and(|current| current != distinct));
    });
}

struct PendingTask;

impl<'d> Fiber<'d> for PendingTask {
    type Output = ();

    fn poll(_: dope_fiber::context::PollCall<'_, '_, 'd, Self>) -> Poll<()> {
        Poll::Pending
    }
}

#[test]
fn task_slab_owns_task_bindings_without_unsafe_callers() {
    Runtime::throughput().with_retained_turn(|turn, mut driver| {
        let reference = driver.driver_ref();
        let targets = reference.targets::<KeyTag<0>>();
        let ready = reference
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("ready slot");
        let parent = RootWaker::from(ready.target());
        let mut slab: Scheduler<'_, PendingTask, usize> =
            Scheduler::try_with_capacity(Capacity::new(1)).expect("scheduler allocation");

        let task = slab.insert(PendingTask, 17, parent).expect("task slot");
        assert_eq!(
            slab.drive_ready(turn.application(), &mut driver, |_, ()| {}),
            0,
        );
        assert!(slab.is_idle());

        assert!(slab.remove(task));
        assert!(slab.is_empty());

        let task = slab
            .insert(PendingTask, 23, parent)
            .expect("reused task slot");
        assert_eq!(
            slab.drive_ready(turn.application(), &mut driver, |_, ()| {}),
            0,
        );
        assert!(slab.remove(task));
        assert!(slab.is_empty());
    });
}

struct ExactWakeProfile;

impl Profile for ExactWakeProfile {
    const QUEUES: QueueLayout = QueueLayout::fixed::<64, 65_536>();
    const SCHEDULER: SchedulerLayout = SchedulerLayout::fixed::<2, 2>();
}

impl Policy for ExactWakeProfile {
    const CONNECT_DEADLINE: Window = Window::from_millis(1);
    const IDLE_WINDOW: Window = Window::from_millis(1);
    const SEND_DEADLINE: Window = Window::from_millis(1);
    const ABS_CONN_AGE: Window = Window::from_millis(1);
}

struct ExactWakeState {
    wait: Pin<Box<Slot>>,
    allowed: Cell<bool>,
    polls: Cell<u32>,
    ticks: Cell<u32>,
    foreign_wakes: Cell<u32>,
}

#[pin_project::pin_project]
struct ExactWakeFiber<'target, 'd> {
    state: &'target ExactWakeState,
    #[pin]
    waiter: Waiter<'target, 'd>,
}

impl<'d> Fiber<'d> for ExactWakeFiber<'_, 'd> {
    type Output = (u32, u32);

    fn poll(call: dope_fiber::context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, context) = call.into_parts();
        let this = this.project();
        let state = &this.state;
        state.polls.set(state.polls.get() + 1);
        if state.allowed.get() {
            return Poll::Ready((state.polls.get(), state.foreign_wakes.get()));
        }
        assert!(
            state
                .wait
                .as_ref()
                .try_register(this.waiter.as_ref(), context.as_ref())
        );
        Poll::Pending
    }
}

struct ExactWakeDispatcher {
    state: Rc<ExactWakeState>,
}

impl<'d> dispatch::Hooks<'d, ()> for ExactWakeDispatcher {
    fn activate(
        &mut self,
        _ready: &mut (),
        target: Token,
        _driver: &mut dope::core::driver::retained::Context<'_, '_, 'd>,
    ) {
        if target.route() == FRAMEWORK
            && target.kind() == 0
            && target.slot() == SlotIndex::ZERO
            && target.epoch() == Some(Epoch::INITIAL)
        {
            self.state
                .foreign_wakes
                .set(self.state.foreign_wakes.get() + 1);
        }
    }

    fn pre_park(
        &mut self,
        _ready: &mut (),
        _driver: &mut dope::core::driver::retained::Context<'_, '_, 'd>,
    ) {
        self.state.ticks.set(self.state.ticks.get() + 1);
        if self.state.ticks.get() == 3 {
            self.state.allowed.set(true);
            self.state.wait.as_ref().wake();
        }
    }

    fn progress(
        &self,
        _ready: &(),
        _region: &o3::cell::region::Token<'d>,
    ) -> dope::core::driver::schedule::Progress<'d> {
        if self.state.allowed.get() {
            dope::core::driver::schedule::Progress::Quiescent
        } else {
            dope::core::driver::schedule::Progress::Runnable
        }
    }
}

#[test]
fn block_on_consumes_only_its_exact_wake_token() {
    Runtime::for_profile::<ExactWakeProfile>().with_session(|mut session| {
        let state = Rc::new(ExactWakeState {
            wait: Box::pin(Slot::new()),
            allowed: Cell::new(false),
            polls: Cell::new(0),
            ticks: Cell::new(0),
            foreign_wakes: Cell::new(0),
        });
        let driver = session.driver_access().driver_ref();
        let foreign_target = driver
            .targets::<dope::core::driver::route::KeyTag<{ FRAMEWORK }>>()
            .bind(SlotIndex::ZERO, Epoch::INITIAL)
            .dispatch();
        let foreign_slot = driver
            .ready()
            .make_ready_slot(foreign_target)
            .expect("ready slot");
        foreign_slot.activate();
        let output = session
            .with_app(
                dispatch::Builder::new(ExactWakeDispatcher {
                    state: Rc::clone(&state),
                })
                .probe::<FRAMEWORK>(),
                |mut app| {
                    app.block_on(ExactWakeFiber {
                        state: state.as_ref(),
                        waiter: Waiter::new(),
                    })
                },
            )
            .expect("application teardown")
            .expect("runtime park");
        assert_eq!(output, (2, 1));
    });
}

struct ShutdownProbe {
    shutdowns: Rc<Cell<usize>>,
    finishes: Rc<Cell<usize>>,
}

impl<'d> dispatch::Hooks<'d, ()> for ShutdownProbe {
    fn shutdown(&mut self, _ready: &mut ()) {
        self.shutdowns.set(self.shutdowns.get() + 1);
    }

    fn finish(&mut self, _ready: &mut ()) {
        self.finishes.set(self.finishes.get() + 1);
    }
}

#[test]
fn app_block_on_is_interrupted_and_finishes_exactly_once() {
    let shutdowns = Rc::new(Cell::new(0));
    let finishes = Rc::new(Cell::new(0));
    let (source, trigger) = shutdown::Pair::new().expect("shutdown pair").split();
    trigger.fire().expect("fire shutdown");
    Runtime::throughput()
        .executor()
        .with_shutdown(source)
        .expect("register shutdown")
        .enter(|mut session| -> io::Result<()> {
            let error = session.with_app(
                dispatch::Builder::new(ShutdownProbe {
                    shutdowns: Rc::clone(&shutdowns),
                    finishes: Rc::clone(&finishes),
                })
                .probe::<0>(),
                |mut app| app.block_on(Pending::<()>::default()).unwrap_err(),
            )?;
            assert_eq!(error.kind(), io::ErrorKind::Interrupted);
            Ok(())
        })
        .expect("runtime shutdown");
    assert_eq!(shutdowns.get(), 1);
    assert_eq!(finishes.get(), 1);
}

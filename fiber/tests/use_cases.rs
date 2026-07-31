#![deny(unsafe_code)]

use std::cell::Cell;
use std::convert::Infallible;
use std::marker::PhantomPinned;
use std::mem::size_of;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::Poll;
use std::time::{Duration, Instant};

use dope::Event;
use dope::driver::profile::DriverProfile;
use dope::driver::ready::{CompletionSlot, CompletionWaker};
use dope::driver::token::{Epoch, ROUTE_FRAMEWORK, SlotIndex, Token};
use dope::manifold::timer::{StarvedWaiter as TimerWaiter, Ticket, Timer};
use dope::runtime::dispatcher::{Dispatcher, Idle};
use dope::runtime::profile::RuntimeProfile;
use dope_fiber::abi::Fiber;
use dope_fiber::abi::batch::Batch;
use dope_fiber::abi::future::lazy::Lazy;
use dope_fiber::abi::pollfn::PollFn;
use dope_fiber::abi::race::{Either, Race};
use dope_fiber::abi::ready::Ready;
use dope_fiber::extensions::SessionExt;
use dope_fiber::io::Io;
use dope_fiber::owner::{SplitBytes, SplitTask};
use dope_fiber::raw::slab::TaskSlab;
use dope_fiber::raw::task::queue::TaskQueue;
use dope_fiber::raw::task::{Context, RootWaker, Waker};
use dope_fiber::raw::wait::{WaitQueue, Waiter};
use dope_fiber::slab::{FixedSlab, Slab};
use dope_fiber::sleep::Sleep;
use dope_fiber::wait::WaitFn;
use dope_net::link::slot::PendingFlags;
use dope_net::wire::identity::Identity;
use dope_test::{
    drain_tokens, poll_ready, poll_with_slot, tok, with_context, with_session, with_session_for,
};
use o3::buffer::Shared;
use o3::cell::BrandCell;
use o3::collections::{FixedPinSlab, PinSlab};

#[test]
fn raw_hot_path_boundaries_add_no_storage() {
    assert_eq!(
        size_of::<Io<'static, 'static, Identity>>(),
        size_of::<(&'static (), Token)>(),
    );
    assert_eq!(size_of::<PendingFlags>(), size_of::<u8>());
    assert_eq!(size_of::<RootWaker<'static>>(), size_of::<[usize; 2]>());
    assert_eq!(size_of::<Waker<'static>>(), size_of::<[usize; 2]>());
    assert_eq!(
        size_of::<Option<RootWaker<'static>>>(),
        size_of::<[usize; 2]>()
    );
    assert_eq!(size_of::<Option<Waker<'static>>>(), size_of::<[usize; 3]>());
    assert_eq!(size_of::<WaitQueue>(), size_of::<[usize; 4]>());
    assert_eq!(
        size_of::<Waiter<'static>>(),
        size_of::<[usize; 3]>() + size_of::<CompletionSlot<'static>>(),
    );
    assert_eq!(
        size_of::<Slab<'static, Ready<()>>>(),
        size_of::<PinSlab<Ready<()>>>(),
    );
    assert_eq!(
        size_of::<FixedSlab<'static, Ready<()>, 1>>(),
        size_of::<FixedPinSlab<Ready<()>, 1>>(),
    );
    assert_eq!(
        size_of::<TaskSlab<'static, Ready<()>>>(),
        size_of::<Slab<'static, Ready<()>>>() + size_of::<Pin<Box<[()]>>>(),
    );
    assert_eq!(
        size_of::<Sleep<'static, 'static>>(),
        size_of::<(
            Instant,
            Option<Ticket>,
            TimerWaiter<'static>,
            &'static Timer<'static>,
        )>(),
    );
}

#[test]
fn batch_reuses_slot_storage_and_accepts_zero_capacity() {
    assert_eq!(
        size_of::<Batch<[u8; 64], [u8; 1], 4>>(),
        size_of::<Batch<[u8; 64], [u8; 64], 4>>(),
    );

    with_context(|mut context| {
        let mut batch = pin!(Batch::<Ready<()>, (), 0>::from_array([]));
        let mut output = poll_ready(batch.as_mut(), context.as_mut());
        assert_eq!(output.next(), None);
    });
}

#[test]
fn waker_identity_is_the_exact_driver_and_wake_target() {
    with_session(|sess| {
        let first = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let second = sess.driver().make_ready_slot(tok(1)).expect("ready slot");
        let registered = Cell::new(Some(Waker::from_ready(sess.driver(), first.key())));
        let same = Waker::from_ready(sess.driver(), first.key());
        let distinct = Waker::from_ready(sess.driver(), second.key());

        assert!(registered.get().is_some_and(|current| current == same));
        assert!(registered.get().is_some_and(|current| current != distinct));
    });
}

struct WakeThenReady(bool);

impl<'d> Fiber<'d> for WakeThenReady {
    type Output = usize;

    fn poll(mut self: Pin<&mut Self>, context: Pin<&mut Context<'_, 'd>>) -> Poll<usize> {
        if self.0 {
            return Poll::Ready(41);
        }
        self.0 = true;
        context.wake();
        Poll::Pending
    }
}

#[test]
fn nested_generated_bridge_preserves_the_exact_root_wake() {
    with_session(|mut session| {
        let slot = session
            .driver()
            .make_ready_slot(tok(7))
            .expect("ready slot");
        let child = dope_fiber::fiber!('_ => async move { WakeThenReady(false).await });
        let mut parent = pin!(dope_fiber::fiber!('_ => async move { child.await + 1 }));

        assert!(poll_with_slot(&mut session, &slot, parent.as_mut()).is_pending());
        assert_eq!(drain_tokens(session.driver()), [tok(7)]);
        assert_eq!(
            poll_with_slot(&mut session, &slot, parent.as_mut()),
            Poll::Ready(42),
        );
    });
}

fn register<'d>(
    queue: Pin<&WaitQueue>,
    waiter: Pin<&Waiter<'d>>,
    wake: CompletionWaker<'d>,
) -> bool {
    queue.try_register_completion(waiter, wake)
}

#[test]
fn request_waiter_can_switch_queues_and_either_endpoint_may_drop_first() {
    with_session(|sess| {
        let request_ready = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let blocker_ready = sess.driver().make_ready_slot(tok(1)).expect("ready slot");
        let canceled_ready = sess.driver().make_ready_slot(tok(2)).expect("ready slot");
        let origin = pin!(WaitQueue::with_capacity(1));
        let saturated = pin!(WaitQueue::with_capacity(1));
        let request = pin!(Waiter::new());
        let blocker = pin!(Waiter::new());

        assert!(register(
            origin.as_ref(),
            request.as_ref(),
            CompletionWaker::from_ready(sess.driver(), request_ready.key()),
        ));
        assert!(register(
            saturated.as_ref(),
            blocker.as_ref(),
            CompletionWaker::from_ready(sess.driver(), blocker_ready.key()),
        ));

        // A failed move must leave the request on its original queue.
        assert!(!register(
            saturated.as_ref(),
            request.as_ref(),
            CompletionWaker::from_ready(sess.driver(), request_ready.key()),
        ));
        origin.as_ref().wake_one();
        assert_eq!(drain_tokens(sess.driver()), [tok(0)]);

        saturated.as_ref().wake_one();
        assert_eq!(drain_tokens(sess.driver()), [tok(1)]);

        // A queue may disappear before a still-pinned request waiter.
        {
            let ephemeral = pin!(WaitQueue::with_capacity(1));
            assert!(register(
                ephemeral.as_ref(),
                request.as_ref(),
                CompletionWaker::from_ready(sess.driver(), request_ready.key()),
            ));
        }
        assert!(!request.is_registered());

        // Cancellation may also drop the waiter while its owner queue lives.
        let canceled = Box::pin(Waiter::new());
        assert!(register(
            origin.as_ref(),
            canceled.as_ref(),
            CompletionWaker::from_ready(sess.driver(), canceled_ready.key()),
        ));
        drop(canceled);
        assert!(origin.is_empty());
        assert!(drain_tokens(sess.driver()).is_empty());
    });
}

#[test]
fn wait_operation_unregisters_on_completion() {
    with_context(|mut context| {
        let queue = pin!(WaitQueue::with_capacity(1));
        let pending = Cell::new(true);
        let operation = WaitFn::new(|context, waiter| {
            if pending.replace(false) {
                assert!(queue.as_ref().try_register(waiter, context.as_ref()));
                return Poll::Pending;
            }
            Poll::Ready(7)
        });
        let mut operation = pin!(operation);

        assert_eq!(
            Fiber::poll(operation.as_mut(), context.as_mut()),
            Poll::Pending
        );
        assert_eq!(queue.len(), 1);
        assert_eq!(
            Fiber::poll(operation.as_mut(), context.as_mut()),
            Poll::Ready(7)
        );
        assert!(queue.is_empty());
    });
}

#[test]
fn race_drops_and_unlinks_the_losing_wait_operation() {
    with_context(|mut context| {
        let queue = pin!(WaitQueue::with_capacity(1));
        let output = {
            let operation = Race::new(
                WaitFn::new(|context, waiter| {
                    assert!(queue.as_ref().try_register(waiter, context.as_ref()));
                    Poll::<()>::Pending
                }),
                Ready::new(17),
            );
            let mut operation = pin!(operation);
            Fiber::poll(operation.as_mut(), context.as_mut())
        };

        assert!(matches!(output, Poll::Ready(Either::Right(17))));
        assert!(queue.is_empty());
    });
}

#[test]
fn lazy_operation_builds_its_fiber_once_on_first_poll() {
    with_context(|mut context| {
        let builds = Cell::new(0);
        let operation = Lazy::new(|| {
            builds.set(builds.get() + 1);
            Ready::new(29)
        });
        let mut operation = pin!(operation);

        assert_eq!(builds.get(), 0);
        assert_eq!(
            Fiber::poll(operation.as_mut(), context.as_mut()),
            Poll::Ready(29)
        );
        assert_eq!(builds.get(), 1);
    });
}

#[test]
fn abi_adapters_do_not_structurally_pin_private_payloads() {
    fn assert_unpin<T: Unpin>(_: &T) {}

    with_context(|mut context| {
        let ready = Ready::new(PhantomPinned);
        assert_unpin(&ready);
        assert_eq!(size_of_val(&ready), size_of::<Option<PhantomPinned>>(),);
        let mut ready = pin!(ready);
        assert!(matches!(
            Fiber::poll(ready.as_mut(), context.as_mut()),
            Poll::Ready(_)
        ));

        let marker = PhantomPinned;
        let poll = PollFn::new(move |_| {
            let _ = &marker;
            Poll::Ready(17)
        });
        assert_unpin(&poll);
        assert_eq!(size_of_val(&poll), size_of::<PhantomPinned>());
        let mut poll = pin!(poll);
        assert_eq!(
            Fiber::poll(poll.as_mut(), context.as_mut()),
            Poll::Ready(17)
        );
    });
}

struct PendingTask;

impl<'d> Fiber<'d> for PendingTask {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: Pin<&mut Context<'_, 'd>>) -> Poll<()> {
        Poll::Pending
    }
}

#[test]
fn persistent_task_binding_owns_both_drop_orders_without_unsafe_callers() {
    with_session(|sess| {
        let ready = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let parent = RootWaker::from_ready(sess.driver(), ready.key());
        let mut slab: TaskSlab<'_, PendingTask, usize> = TaskSlab::with_capacity(1);

        let task = slab.insert(PendingTask).expect("task slot");
        let queue = Box::pin(TaskQueue::with_capacity(1));
        assert!(slab.bind(&task, queue.as_ref(), 17, parent));
        assert!(slab.wake(&task));
        assert_eq!(
            queue
                .as_ref()
                .snapshot_root(parent)
                .expect("ready snapshot")
                .next(),
            Some(17),
        );
        assert_eq!(drain_tokens(sess.driver()), [tok(0)]);

        // A connection queue may disappear before its scheduler entry.
        drop(queue);
        assert!(!slab.wake(&task));
        assert!(slab.remove(task));

        // Cancellation may remove the scheduler entry while its queue lives.
        let task = slab.insert(PendingTask).expect("reused task slot");
        let queue = Box::pin(TaskQueue::with_capacity(1));
        assert!(slab.bind(&task, queue.as_ref(), 23, parent));
        assert!(slab.remove(task));
        assert!(queue.as_ref().is_empty());
        assert!(drain_tokens(sess.driver()).is_empty());
    });
}

struct BorrowedTask<'a> {
    head: &'a [u8],
    body: &'a [u8],
    pending: bool,
}

struct BorrowSplitTask;

impl<'d> SplitTask<'d> for BorrowSplitTask {
    type Input = bool;
    type State = ();
    type Context = ();
    type Output = usize;
    type Error = Infallible;

    fn build<'req>(
        view: dope_fiber::owner::SplitView<'req>,
        pending: Self::Input,
        _state: &'req Self::State,
        _context: &'req Self::Context,
    ) -> Result<impl Fiber<'d, Output = Self::Output> + 'req, Self::Error>
    where
        'd: 'req,
    {
        let (head, body) = view.into_parts();
        Ok(BorrowedTask {
            head,
            body,
            pending,
        })
    }
}

impl<'d> Fiber<'d> for BorrowedTask<'_> {
    type Output = usize;

    fn poll(mut self: Pin<&mut Self>, _: Pin<&mut Context<'_, 'd>>) -> Poll<usize> {
        if self.pending {
            self.pending = false;
            return Poll::Pending;
        }
        assert_eq!(self.head, b"head");
        assert_eq!(self.body, b"body");
        Poll::Ready(self.head.len() + self.body.len())
    }
}

impl Drop for BorrowedTask<'_> {
    fn drop(&mut self) {
        assert_eq!(self.head, b"head");
        assert_eq!(self.body, b"body");
    }
}

#[test]
fn split_task_borrows_owned_bytes_across_polls_without_extra_state() {
    with_session(|mut session| {
        let ready = session
            .driver()
            .make_ready_slot(tok(0))
            .expect("ready slot");
        let owner = SplitBytes::new(Shared::copy_from_slice(b"headbody"), None, 4);
        let state = ();
        let context = ();
        let task = owner
            .try_into_task::<BorrowSplitTask>(true, &state, &context)
            .expect("infallible split task");
        assert_eq!(
            size_of_val(&task),
            size_of::<(BorrowedTask<'static>, SplitBytes)>(),
        );
        let mut task = pin!(task);

        assert_eq!(
            poll_with_slot(&mut session, &ready, task.as_mut()),
            Poll::Pending
        );
        assert_eq!(
            poll_with_slot(&mut session, &ready, task.as_mut()),
            Poll::Ready(8)
        );
    });
}

#[test]
fn fiber_slab_accepts_a_fiber_borrowing_a_lexical_session_local() {
    with_context(|mut context| {
        let request = *b"headbody";
        let task = BorrowedTask {
            head: &request[..4],
            body: &request[4..],
            pending: false,
        };
        let mut slab: Slab<'_, BorrowedTask<'_>> = Slab::with_capacity(1);
        let id = slab.insert(task).expect("task slot");

        assert_eq!(slab.poll(&id, context.as_mut()), Some(Poll::Ready(8)));
        assert!(slab.remove(id));
    });
}

struct ExactWakeProfile;

impl DriverProfile for ExactWakeProfile {
    const RING_ENTRIES: u32 = 64;
    const READY_SLOTS: usize = 2;
}

impl RuntimeProfile for ExactWakeProfile {
    const IDLE_WINDOW: Duration = Duration::ZERO;
}

struct ExactWakeState {
    wait: Pin<Box<WaitQueue>>,
    allowed: Cell<bool>,
    polls: Cell<u32>,
    ticks: Cell<u32>,
    foreign_wakes: Cell<u32>,
    foreign: Token,
}

#[pin_project::pin_project]
struct ExactWakeFiber<'d> {
    state: Rc<ExactWakeState>,
    #[pin]
    waiter: Waiter<'d>,
}

impl<'d> Fiber<'d> for ExactWakeFiber<'d> {
    type Output = (u32, u32);

    fn poll(self: Pin<&mut Self>, context: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.project();
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

impl<'d> Dispatcher<'d> for ExactWakeDispatcher {
    fn dispatch(
        self: Pin<&mut Self>,
        _event: Event<'d>,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) {
    }

    fn activate(self: Pin<&mut Self>, target: Token, _driver: &mut dope::DriverContext<'_, 'd>) {
        let state = &self.as_ref().get_ref().state;
        if target == state.foreign {
            state.foreign_wakes.set(state.foreign_wakes.get() + 1);
        }
    }

    fn pre_park(self: Pin<&mut Self>, _driver: &mut dope::DriverContext<'_, 'd>) {
        let state = &self.as_ref().get_ref().state;
        state.ticks.set(state.ticks.get() + 1);
        if state.ticks.get() == 3 {
            state.allowed.set(true);
            state.wait.as_ref().wake();
        }
    }

    fn idle(self: Pin<&Self>) -> Idle {
        Idle::Busy
    }
}

#[test]
fn block_on_consumes_only_its_exact_wake_token() {
    with_session_for::<ExactWakeProfile, _>(|mut session| {
        let foreign = Token::new(ROUTE_FRAMEWORK, SlotIndex::ZERO, Epoch::INITIAL);
        let state = Rc::new(ExactWakeState {
            wait: Box::pin(WaitQueue::with_capacity(1)),
            allowed: Cell::new(false),
            polls: Cell::new(0),
            ticks: Cell::new(0),
            foreign_wakes: Cell::new(0),
            foreign,
        });
        let foreign_slot = session
            .driver()
            .make_ready_slot(foreign)
            .expect("ready slot");
        foreign_slot.activate();
        let app = pin!(BrandCell::new(ExactWakeDispatcher {
            state: Rc::clone(&state),
        }));
        let output = session
            .block_on(
                app.as_ref(),
                ExactWakeFiber {
                    state: Rc::clone(&state),
                    waiter: Waiter::new(),
                },
            )
            .expect("runtime park");
        assert_eq!(output, (2, 1));
    });
}

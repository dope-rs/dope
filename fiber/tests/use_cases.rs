#![deny(unsafe_code)]

use std::cell::RefCell;
use std::convert::Infallible;
use std::pin::{Pin, pin};
use std::task::Poll;

use dope_fiber::{
    Context, Fiber, FiberScope, OwnerFiber, RootWaker, Slab, SplitBytes, SplitTask, TaskQueue,
    TaskSlab, WaitQueue, Waiter, Waker, try_from_split_task,
};
use dope_test::{drain_tokens, poll_with_slot, tok, with_context, with_session};
use o3::buffer::Shared;

fn register<'d>(queue: Pin<&WaitQueue>, waiter: Pin<&Waiter<'d>>, waker: Waker<'d>) -> bool {
    queue.try_register_waker(waiter, waker)
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
            Waker::from_ready(sess.driver(), request_ready.key()),
        ));
        assert!(register(
            saturated.as_ref(),
            blocker.as_ref(),
            Waker::from_ready(sess.driver(), blocker_ready.key()),
        ));

        // A failed move must leave the request on its original queue.
        assert!(!register(
            saturated.as_ref(),
            request.as_ref(),
            Waker::from_ready(sess.driver(), request_ready.key()),
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
                Waker::from_ready(sess.driver(), request_ready.key()),
            ));
        }
        assert!(!request.is_registered());

        // Cancellation may also drop the waiter while its owner queue lives.
        let canceled = Box::pin(Waiter::new());
        assert!(register(
            origin.as_ref(),
            canceled.as_ref(),
            Waker::from_ready(sess.driver(), canceled_ready.key()),
        ));
        drop(canceled);
        assert!(origin.is_empty());
        assert!(drain_tokens(sess.driver()).is_empty());
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
        let mut slab: TaskSlab<'_, PendingTask, usize> = TaskSlab::with_capacity(1, 0usize);

        let task = slab.insert(PendingTask).expect("task slot");
        let queue = Box::pin(TaskQueue::with_capacity(1));
        assert!(slab.bind(&task, queue.as_ref(), 17, parent));
        assert!(slab.wake(&task));
        assert_eq!(queue.as_ref().pop(), Some(17));
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
        view: dope_fiber::SplitView<'req>,
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

struct DropProbe<'a> {
    name: &'static str,
    log: &'a RefCell<Vec<&'static str>>,
}

impl Drop for DropProbe<'_> {
    fn drop(&mut self) {
        self.log.borrow_mut().push(self.name);
    }
}

#[test]
#[allow(unsafe_code)]
fn owner_backed_fiber_borrows_across_polls_and_drops_before_its_owner() {
    with_session(|mut session| {
        let ready = session
            .driver()
            .make_ready_slot(tok(0))
            .expect("ready slot");
        let request = Shared::copy_from_slice(b"headbody");
        let owner = SplitBytes::new(request, None, 4);
        // SAFETY: the view is moved only into `BorrowedTask`; the closure has
        // no side channel through which it could escape.
        let task = unsafe {
            OwnerFiber::try_from_split(owner, FiberScope::from_driver(session.driver()), |view| {
                let (head, body) = view.into_parts();
                Ok::<_, Infallible>(BorrowedTask {
                    head,
                    body,
                    pending: true,
                })
            })
        }
        .expect("infallible owner-backed construction");
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

    let log = RefCell::new(Vec::new());
    drop(OwnerFiber::from_parts(
        DropProbe {
            name: "fiber",
            log: &log,
        },
        DropProbe {
            name: "owner",
            log: &log,
        },
    ));
    assert_eq!(&*log.borrow(), &["fiber", "owner"]);
    assert_eq!(
        size_of::<OwnerFiber<[usize; 4], ()>>(),
        size_of::<[usize; 4]>(),
    );
    assert_eq!(size_of::<FiberScope<'_>>(), 0);
}

#[test]
fn safe_split_task_borrows_owned_bytes_across_polls() {
    with_session(|mut session| {
        let ready = session
            .driver()
            .make_ready_slot(tok(0))
            .expect("ready slot");
        let owner = SplitBytes::new(Shared::copy_from_slice(b"headbody"), None, 4);
        let state = ();
        let context = ();
        let task = try_from_split_task::<BorrowSplitTask>(owner, true, &state, &context)
            .expect("infallible split task");
        assert_eq!(
            size_of_val(&task),
            size_of::<OwnerFiber<BorrowedTask<'static>, SplitBytes>>(),
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

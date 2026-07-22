use std::cell::Cell;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::Poll;

use dope_fiber::{
    Batch, Context, Fiber, FixedSlab, Slab, TaskContext, TaskId, TaskQueue, WaitQueue, Waiter,
    Waker, ready,
};
use dope_test::{
    allocations_during, assert_unwinds, counter, drain_tokens, poll_ready, tok, with_context,
    with_session,
};

struct PendingCount(Rc<Cell<usize>>);

impl<'d> Fiber<'d> for PendingCount {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: Pin<&mut Context<'_, 'd>>) -> Poll<()> {
        self.0.set(self.0.get() + 1);
        Poll::Pending
    }
}

struct Probe {
    polls: Rc<Cell<usize>>,
    ready: bool,
}

impl<'d> Fiber<'d> for Probe {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, _: Pin<&mut Context<'_, 'd>>) -> Poll<usize> {
        self.polls.set(self.polls.get() + 1);
        if self.ready {
            Poll::Ready(1)
        } else {
            Poll::Pending
        }
    }
}

struct Controlled<'d> {
    polls: Rc<Cell<usize>>,
    ready: Rc<Cell<bool>>,
    waker: Rc<Cell<Option<Waker<'d>>>>,
    output: usize,
}

impl<'d> Fiber<'d> for Controlled<'d> {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<usize> {
        self.polls.set(self.polls.get() + 1);
        if self.ready.get() {
            Poll::Ready(self.output)
        } else {
            self.waker.set(Some(unsafe { cx.waker_unchecked() }));
            Poll::Pending
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum PanicSite {
    Poll,
    FiberDrop,
    OutputDrop,
}

struct ProbeOutput {
    drops: Rc<Cell<usize>>,
    value: usize,
    panic: bool,
}

impl Drop for ProbeOutput {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        assert!(!self.panic, "output drop panic");
    }
}

struct DropProbe {
    fiber_drops: Rc<Cell<usize>>,
    output_drops: Rc<Cell<usize>>,
    output: Option<usize>,
    site: Option<PanicSite>,
}

impl DropProbe {
    fn new(
        fiber_drops: &Rc<Cell<usize>>,
        output_drops: &Rc<Cell<usize>>,
        output: usize,
        site: Option<PanicSite>,
    ) -> Self {
        Self {
            fiber_drops: Rc::clone(fiber_drops),
            output_drops: Rc::clone(output_drops),
            output: Some(output),
            site,
        }
    }
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.fiber_drops.set(self.fiber_drops.get() + 1);
        assert!(self.site != Some(PanicSite::FiberDrop), "fiber drop panic");
    }
}

impl<'d> Fiber<'d> for DropProbe {
    type Output = ProbeOutput;

    fn poll(self: Pin<&mut Self>, _: Pin<&mut Context<'_, 'd>>) -> Poll<ProbeOutput> {
        let this = unsafe { self.get_unchecked_mut() };
        assert!(this.site != Some(PanicSite::Poll), "poll panic");
        Poll::Ready(ProbeOutput {
            drops: Rc::clone(&this.output_drops),
            value: this.output.take().expect("drop fiber repolled"),
            panic: this.site == Some(PanicSite::OutputDrop),
        })
    }
}

struct PanicDrop(bool);

impl<'d> Fiber<'d> for PanicDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _context: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for PanicDrop {
    fn drop(&mut self) {
        if self.0 {
            panic!("drop");
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WideTarget(u128);

#[test]
fn bounded_capacity_rejects_excess_input() {
    let polls = counter();
    let mut batch = Batch::<Probe, usize, 2>::new();
    assert!(
        batch
            .try_push(Probe {
                polls: Rc::clone(&polls),
                ready: true,
            })
            .is_ok()
    );
    assert!(
        batch
            .try_push(Probe {
                polls: Rc::clone(&polls),
                ready: true,
            })
            .is_ok()
    );
    assert!(batch.try_push(Probe { polls, ready: true }).is_err());
    assert_eq!(batch.capacity(), 2);
    assert_eq!(batch.len(), 2);
}

#[test]
fn newly_admitted_pending_fibers_are_polled_once() {
    with_context(|mut cx| {
        let counts: [Rc<Cell<usize>>; 4] = std::array::from_fn(|_| counter());
        let fibers = counts.clone().map(PendingCount);
        let mut batch = std::pin::pin!(Batch::from_array(fibers));

        assert!(Fiber::poll(batch.as_mut(), cx.as_mut()).is_pending());
        assert!(counts.iter().all(|count| count.get() == 1));
    });
}

#[test]
fn ready_batch_yields_after_completion_budget() {
    with_context(|mut cx| {
        let polls = counter();
        let fibers: [Probe; 40] = std::array::from_fn(|_| Probe {
            polls: Rc::clone(&polls),
            ready: true,
        });
        let mut batch = std::pin::pin!(Batch::from_array(fibers));

        assert!(Fiber::poll(batch.as_mut(), cx.as_mut()).is_pending());
        assert_eq!(polls.get(), 32);
        let output = poll_ready(batch.as_mut(), cx.as_mut());
        assert_eq!(polls.get(), 40);
        assert_eq!(output.collect::<Vec<_>>(), vec![1; 40]);
    });
}

#[test]
fn pending_batch_polls_only_explicitly_woken_children() {
    with_context(|mut cx| {
        let first_polls = counter();
        let second_polls = counter();
        let first_ready = Rc::new(Cell::new(false));
        let second_ready = Rc::new(Cell::new(false));
        let first_waker = Rc::new(Cell::new(None));
        let second_waker = Rc::new(Cell::new(None));
        let fibers = [
            Controlled {
                polls: Rc::clone(&first_polls),
                ready: Rc::clone(&first_ready),
                waker: Rc::clone(&first_waker),
                output: 1,
            },
            Controlled {
                polls: Rc::clone(&second_polls),
                ready: Rc::clone(&second_ready),
                waker: Rc::clone(&second_waker),
                output: 2,
            },
        ];
        let mut batch = std::pin::pin!(Batch::from_array(fibers));

        assert!(Fiber::poll(batch.as_mut(), cx.as_mut()).is_pending());
        assert_eq!(first_polls.get(), 1);
        assert_eq!(second_polls.get(), 1);

        first_ready.set(true);
        first_waker.take().expect("first waker").wake();
        assert!(Fiber::poll(batch.as_mut(), cx.as_mut()).is_pending());
        assert_eq!(first_polls.get(), 2);
        assert_eq!(second_polls.get(), 1);

        second_ready.set(true);
        second_waker.take().expect("second waker").wake();
        let output = poll_ready(batch.as_mut(), cx.as_mut());
        assert_eq!(second_polls.get(), 2);
        assert_eq!(output.collect::<Vec<_>>(), vec![1, 2]);
    });
}

#[test]
fn generated_inline_batch_await_allocates_nothing() {
    with_context(|mut cx| {
        let (warm_allocations, _) = allocations_during(|| ());
        assert_eq!(warm_allocations, 0);

        let mut sum = 0;
        let (allocations, _) = allocations_during(|| {
            let fiber = dope_gen::fiber!('_ => async move {
                let fibers = core::array::from_fn(dope_fiber::ready);
                let outputs = Batch::<_, usize, 8>::from_array(fibers).await;
                outputs.sum::<usize>()
            });
            let mut fiber = std::pin::pin!(fiber);
            sum = poll_ready(fiber.as_mut(), cx.as_mut());
        });

        assert_eq!(sum, 28);
        assert_eq!(allocations, 0);
    });
}

#[test]
fn completed_fibers_and_unconsumed_outputs_drop_once() {
    with_context(|mut cx| {
        let fiber_drops = counter();
        let output_drops = counter();
        let fibers =
            core::array::from_fn(|value| DropProbe::new(&fiber_drops, &output_drops, value, None));
        let mut batch = std::pin::pin!(Batch::<_, ProbeOutput, 3>::from_array(fibers));
        let mut outputs = poll_ready(batch.as_mut(), cx.as_mut());

        assert_eq!(fiber_drops.get(), 3);
        let first = outputs.next().expect("first output");
        assert_eq!(first.value, 0);
        drop(first);
        assert_eq!(output_drops.get(), 1);
        drop(outputs);
        assert_eq!(output_drops.get(), 3);
    });
}

#[test]
fn poll_panic_poisoning_preserves_drop_safety() {
    with_context(|mut cx| {
        let drops = counter();
        let output_drops = counter();

        {
            let mut batch = std::pin::pin!(Batch::<_, ProbeOutput, 2>::from_array([
                DropProbe::new(&drops, &output_drops, 0, Some(PanicSite::Poll)),
                DropProbe::new(&drops, &output_drops, 1, Some(PanicSite::Poll)),
            ]));
            assert_unwinds(|| Fiber::poll(batch.as_mut(), cx.as_mut()));
            assert_unwinds(|| Fiber::poll(batch.as_mut(), cx.as_mut()));
        }

        assert_eq!(drops.get(), 2);
    });
}

#[test]
fn output_drop_panic_releases_the_remaining_array() {
    with_context(|mut cx| {
        let drops = counter();
        let fibers = core::array::from_fn(|index| {
            dope_fiber::ready(ProbeOutput {
                drops: Rc::clone(&drops),
                value: 0,
                panic: index == 0,
            })
        });
        let mut batch = std::pin::pin!(Batch::<_, ProbeOutput, 3>::from_array(fibers));
        let outputs = poll_ready(batch.as_mut(), cx.as_mut());

        assert_unwinds(|| drop(outputs));
        assert_eq!(drops.get(), 3);
    });
}

#[test]
fn fiber_drop_panic_cannot_redrop_and_keeps_output_live() {
    with_context(|mut cx| {
        let fiber_drops = counter();
        let output_drops = counter();

        {
            let mut batch =
                std::pin::pin!(Batch::<_, ProbeOutput, 1>::from_array([DropProbe::new(
                    &fiber_drops,
                    &output_drops,
                    0,
                    Some(PanicSite::FiberDrop)
                ),]));
            assert_unwinds(|| Fiber::poll(batch.as_mut(), cx.as_mut()));
            assert_eq!(fiber_drops.get(), 1);
            assert_eq!(output_drops.get(), 0);
        }

        assert_eq!(fiber_drops.get(), 1);
        assert_eq!(output_drops.get(), 1);
    });
}

#[test]
fn dynamic_fiber_slab_validates_generations() {
    with_context(|mut context| {
        let mut slab: Slab<'_, _> = Slab::with_capacity(1);
        let task = slab
            .vacant_entry()
            .expect("new dynamic fiber slab should have a vacant task slot")
            .insert(ready(7));
        assert!(slab.insert(ready(9)).is_none());
        let erased = task.erase();
        assert_eq!(erased.index(), 0);
        let task = TaskId::from_erased(erased);
        let stale = TaskId::from_erased(erased);
        assert_eq!(slab.poll(&task, context.as_mut()), Some(Poll::Ready(7)));
        assert!(slab.remove(task));

        let next = slab.insert(ready(11)).unwrap();
        assert!(slab.poll(&stale, context.as_mut()).is_none());
        assert!(!slab.remove(stale));
        assert!(slab.remove(next));
    });
}

#[test]
fn fixed_fiber_slab_validates_generations() {
    with_context(|mut context| {
        let mut slab = pin!(FixedSlab::<'_, _, 1>::new());
        let task = slab
            .as_mut()
            .vacant_entry()
            .expect("new fixed fiber slab should have a vacant task slot")
            .insert(ready(7));
        let erased = task.erase();
        let task = TaskId::from_erased(erased);
        let stale = TaskId::from_erased(erased);
        assert_eq!(
            slab.as_mut().poll(&task, context.as_mut()),
            Some(Poll::Ready(7))
        );
        assert!(slab.as_mut().remove(task));

        let next = slab.as_mut().insert(ready(11)).unwrap();
        assert!(slab.as_mut().poll(&stale, context.as_mut()).is_none());
        assert!(!slab.as_mut().remove(stale));
        assert!(slab.as_mut().remove(next));
    });
}

#[test]
fn fiber_slabs_contain_drop_panics() {
    let dynamic = std::panic::catch_unwind(|| {
        let mut slab: Slab<'_, _> = Slab::with_capacity(2);
        slab.insert(PanicDrop(true)).unwrap();
        slab.insert(PanicDrop(true)).unwrap();
        drop(slab);
    });
    assert!(dynamic.is_ok());

    let fixed = std::panic::catch_unwind(|| {
        let mut slab = pin!(FixedSlab::<'static, _, 1>::new());
        let task = slab.as_mut().insert(PanicDrop(true)).unwrap();
        assert!(slab.as_mut().remove(task));
        let replacement = slab.as_mut().insert(PanicDrop(false)).unwrap();
        assert!(slab.as_mut().remove(replacement));
    });
    assert!(fixed.is_ok());

    let fixed = std::panic::catch_unwind(|| {
        let mut slab = pin!(FixedSlab::<'static, _, 2>::new());
        slab.as_mut().insert(PanicDrop(true)).unwrap();
        slab.as_mut().insert(PanicDrop(true)).unwrap();
    });
    assert!(fixed.is_ok());
}

#[test]
fn wait_queue_deduplicates_and_reports_overflow() {
    with_session(|sess| {
        let first = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let second = sess.driver().make_ready_slot(tok(1)).expect("ready slot");
        let overflow = sess.driver().make_ready_slot(tok(2)).expect("ready slot");
        let queue = pin!(WaitQueue::with_capacity(2));
        let first_waiter = pin!(Waiter::new());
        let second_waiter = pin!(Waiter::new());
        let overflow_waiter = pin!(Waiter::new());
        let first_waker = Waker::from_ready(sess.driver(), first.key());
        let second_waker = Waker::from_ready(sess.driver(), second.key());
        let overflow_waker = Waker::from_ready(sess.driver(), overflow.key());
        assert!(
            queue
                .as_ref()
                .try_register_waker(first_waiter.as_ref(), first_waker)
        );
        assert!(
            queue
                .as_ref()
                .try_register_waker(first_waiter.as_ref(), first_waker)
        );
        assert!(
            queue
                .as_ref()
                .try_register_waker(second_waiter.as_ref(), second_waker)
        );
        assert!(queue.as_ref().can_register(first_waiter.as_ref()));
        assert!(!queue.as_ref().can_register(overflow_waiter.as_ref()));
        assert!(
            !queue
                .as_ref()
                .try_register_waker(overflow_waiter.as_ref(), overflow_waker)
        );

        assert!(drain_tokens(sess.driver()).is_empty());
        queue.as_ref().wake();
        let mut out = drain_tokens(sess.driver());
        out.sort_unstable_by_key(|token| token.raw());
        assert_eq!(out, [tok(0), tok(1)]);
    });
}

#[test]
fn dropping_waiter_unlinks_without_disturbing_order() {
    with_session(|sess| {
        let first = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let second = sess.driver().make_ready_slot(tok(1)).expect("ready slot");
        let removed = sess.driver().make_ready_slot(tok(2)).expect("ready slot");
        let wrapped = sess.driver().make_ready_slot(tok(3)).expect("ready slot");
        let queue = pin!(WaitQueue::with_capacity(3));
        let first_waiter = Box::pin(Waiter::new());
        let second_waiter = Box::pin(Waiter::new());
        let removed_waiter = Box::pin(Waiter::new());
        let wrapped_waiter = Box::pin(Waiter::new());
        let first_waker = Waker::from_ready(sess.driver(), first.key());
        let second_waker = Waker::from_ready(sess.driver(), second.key());
        let removed_waker = Waker::from_ready(sess.driver(), removed.key());
        let wrapped_waker = Waker::from_ready(sess.driver(), wrapped.key());

        assert!(
            queue
                .as_ref()
                .try_register_waker(first_waiter.as_ref(), first_waker)
        );
        assert!(
            queue
                .as_ref()
                .try_register_waker(second_waiter.as_ref(), second_waker)
        );
        assert!(
            queue
                .as_ref()
                .try_register_waker(removed_waiter.as_ref(), removed_waker)
        );
        queue.as_ref().wake_one();
        assert!(
            queue
                .as_ref()
                .try_register_waker(wrapped_waiter.as_ref(), wrapped_waker)
        );
        drop(removed_waiter);
        queue.as_ref().wake();

        assert_eq!(drain_tokens(sess.driver()), [tok(0), tok(1), tok(3)]);
    });
}

#[test]
fn waiter_survives_queue_drop_without_a_dangling_registration() {
    with_session(|sess| {
        let ready = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let waiter = pin!(Waiter::new());
        {
            let queue = pin!(WaitQueue::with_capacity(1));
            let waker = Waker::from_ready(sess.driver(), ready.key());
            assert!(queue.as_ref().try_register_waker(waiter.as_ref(), waker));
        }
        assert!(!waiter.is_registered());
        assert!(!waiter.as_ref().unregister());
        let replacement = pin!(WaitQueue::with_capacity(1));
        let waker = Waker::from_ready(sess.driver(), ready.key());
        assert!(
            replacement
                .as_ref()
                .try_register_waker(waiter.as_ref(), waker)
        );
    });
}

#[test]
fn child_queue_wakes_parent_once() {
    with_session(|sess| {
        let parent = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let queue = pin!(TaskQueue::new());
        let task = pin!(TaskContext::new());
        let binding = task.as_ref().bind(
            queue.as_ref(),
            17,
            Some(Waker::from_ready(sess.driver(), parent.key())),
        );
        let child = binding.waker();
        child.wake();
        child.wake();
        assert_eq!(queue.as_ref().pop(), Some(17));
        assert!(queue.as_ref().is_empty());
        assert_eq!(drain_tokens(sess.driver()), [tok(0)]);
    });
}

#[test]
fn wake_of_an_unpolled_batch_member_is_coalesced() {
    with_session(|sess| {
        let parent = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let parent_waker = Waker::from_ready(sess.driver(), parent.key());
        let queue = pin!(TaskQueue::new());
        let first = pin!(TaskContext::new());
        let second = pin!(TaskContext::new());
        let first_binding = first.as_ref().bind(queue.as_ref(), 10, Some(parent_waker));
        let second_binding = second.as_ref().bind(queue.as_ref(), 11, Some(parent_waker));
        let first_waker = first_binding.waker();
        let second_waker = second_binding.waker();

        first_waker.wake();
        second_waker.wake();
        assert_eq!(drain_tokens(sess.driver()), [tok(0)]);

        let mut batch = queue
            .as_ref()
            .snapshot(parent_waker.shorten())
            .expect("single live snapshot");
        assert_eq!(batch.next(), Some(10));
        second_waker.wake();
        assert_eq!(batch.next(), Some(11));
        assert_eq!(batch.next(), None);
        drop(batch);

        assert!(
            queue
                .as_ref()
                .snapshot(parent_waker.shorten())
                .expect("previous snapshot dropped")
                .next()
                .is_none()
        );
        assert!(drain_tokens(sess.driver()).is_empty());

        drop(first_binding);
        drop(second_binding);
    });
}

#[test]
fn task_queue_preserves_target_type() {
    let target = WideTarget(u128::MAX - 17);
    let queue = pin!(TaskQueue::<WideTarget>::new());
    let task = pin!(TaskContext::with_target(WideTarget(0)));
    {
        let binding = task.as_ref().bind(queue.as_ref(), target, None);
        binding.wake();
        assert_eq!(queue.as_ref().pop(), Some(target));
    };
}

#[test]
fn unbind_unlinks_queued_child() {
    with_session(|sess| {
        let parent = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let queue = pin!(TaskQueue::new());
        let task = pin!(TaskContext::new());
        let binding = task.as_ref().bind(
            queue.as_ref(),
            19,
            Some(Waker::from_ready(sess.driver(), parent.key())),
        );
        let child = binding.waker();
        child.wake();
        drop(binding);
        assert!(queue.as_ref().is_empty());
    });
}

#[test]
fn dropped_slot_is_unlinked() {
    with_session(|sess| {
        {
            let slot = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
            slot.activate();
        }
        assert!(drain_tokens(sess.driver()).is_empty());
    });
}

#[test]
fn stale_ready_key_cannot_activate_reused_slot() {
    with_session(|sess| {
        let stale = {
            let slot = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
            slot.key()
        };
        let replacement = sess.driver().make_ready_slot(tok(1)).expect("ready slot");
        sess.driver().activate_ready(stale);
        assert!(drain_tokens(sess.driver()).is_empty());
        replacement.activate();
        assert_eq!(drain_tokens(sess.driver()), [tok(1)]);
    });
}

#[test]
fn queued_slot_coalesces_latest_target() {
    with_session(|sess| {
        let slot = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        slot.activate();
        slot.set_target(tok(1));
        slot.activate();
        assert_eq!(drain_tokens(sess.driver()), [tok(1)]);
    });
}

#[test]
fn wake_of_an_unpolled_ready_slot_is_coalesced() {
    with_session(|sess| {
        let targets = [tok(0), tok(1)];
        let slots = sess
            .driver()
            .make_ready_slots(targets)
            .expect("ready slots");
        let first = slots.first().unwrap();
        let second = slots.get(1).unwrap();
        first.activate();
        second.activate();

        let mut drained = Vec::new();
        sess.driver().drain_ready(|target| {
            drained.push(target);
            if target == targets[0] {
                second.activate();
            }
        });

        assert_eq!(drained, targets);
        assert!(drain_tokens(sess.driver()).is_empty());
    });
}

#[test]
fn wake_after_dequeue_is_deferred_to_the_next_batch() {
    with_session(|sess| {
        let target = tok(0);
        let slot = sess.driver().make_ready_slot(target).expect("ready slot");
        slot.activate();

        let mut drained = Vec::new();
        sess.driver().drain_ready(|ready| {
            drained.push(ready);
            slot.activate();
        });

        assert_eq!(drained, [target]);
        assert_eq!(drain_tokens(sess.driver()), [target]);
    });
}

#[test]
fn drain_requeues_after_unwind() {
    with_session(|sess| {
        let targets = [tok(0), tok(1), tok(2)];
        let slots = sess
            .driver()
            .make_ready_slots(targets)
            .expect("ready slots");
        for index in 0..slots.len() {
            slots.get(index).unwrap().activate();
        }
        let first = Waker::from_ready(sess.driver(), slots.first().unwrap().key());
        assert_unwinds(|| {
            sess.driver().drain_ready(|_| {
                panic!("wake");
            });
        });
        first.wake();
        assert_eq!(
            drain_tokens(sess.driver()),
            [targets[1], targets[2], targets[0]]
        );
    });
}

#[test]
fn recursive_drain_defers_new_wakes() {
    with_session(|sess| {
        let first = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let deferred = sess.driver().make_ready_slot(tok(1)).expect("ready slot");
        first.activate();
        let nested = std::cell::Cell::new(None);
        sess.driver().drain_ready(|target| {
            assert_eq!(target, tok(0));
            deferred.activate();
            sess.driver().drain_ready(|target| nested.set(Some(target)));
        });
        assert_eq!(nested.get(), None);
        assert_eq!(drain_tokens(sess.driver()), [tok(1)]);
    });
}

#[test]
fn dropping_pending_snapshot_slot_unlinks_it() {
    with_session(|sess| {
        let targets = [tok(0), tok(1), tok(2)];
        let first = sess
            .driver()
            .make_ready_slot(targets[0])
            .expect("ready slot");
        let mut second = Some(
            sess.driver()
                .make_ready_slot(targets[1])
                .expect("ready slot"),
        );
        let third = sess
            .driver()
            .make_ready_slot(targets[2])
            .expect("ready slot");
        first.activate();
        second.as_ref().unwrap().activate();
        third.activate();
        let mut out = Vec::new();
        sess.driver().drain_ready(|target| {
            out.push(target);
            if target == targets[0] {
                drop(second.take());
            }
        });
        assert_eq!(out, [targets[0], targets[2]]);
    });
}

use std::{ops::ControlFlow, pin, task::Poll};

use o3::mem::{Lease, Return, lease};

use crate::{
    context::{self, wake},
    core::driver::{
        self,
        schedule::{app, app::step},
    },
    task::storage::fixed::schedule::{
        pool::{Mode, Slots},
        queue,
        ready::raw,
        result::Outcome,
    },
};

#[must_use = "dropping a selected task returns it to the ready set"]
pub struct Item<
    'plane,
    'turn,
    'd,
    F,
    T: Copy,
    M: Mode<'d, F>,
    S: queue::Backend<'d> + 'plane = queue::Flat<'d>,
> {
    state: Lease<State<'plane, 'turn, 'd, F, T, M, S>, lease::Open>,
}

struct State<'plane, 'turn, 'd, F, T: Copy, M, S: queue::Backend<'d> + 'plane> {
    slots: pin::Pin<&'plane mut Slots<'d, F, T, M, S>>,
    dequeued: S::Dequeued<'plane>,
    work: app::Work<'turn, 'd>,
    permit: app::Permit<'turn, 'd>,
}

impl<'plane, 'turn, 'd, F, T: Copy, M: Mode<'d, F>, S: queue::Backend<'d> + 'plane>
    Item<'plane, 'turn, 'd, F, T, M, S>
{
    pub(super) fn new(
        slots: pin::Pin<&'plane mut Slots<'d, F, T, M, S>>,
        dequeued: S::Dequeued<'plane>,
        work: app::Work<'turn, 'd>,
        permit: app::Permit<'turn, 'd>,
    ) -> Self {
        Self {
            state: Lease::open(
                State {
                    slots,
                    dequeued,
                    work,
                    permit,
                },
                (),
            ),
        }
    }

    pub fn target(&self) -> T {
        let state = self.state.sink();
        // SAFETY: the private selection retains this exact storage and queue loan.
        unsafe { raw::get(state.slots.as_ref().get_ref(), S::index(&state.dequeued)) }.target
    }

    fn drive_once(
        self,
        driver: &mut driver::Context<'_, 'd>,
    ) -> (
        pin::Pin<&'plane mut Slots<'d, F, T, M, S>>,
        Outcome<T, M::Output, M::Item>,
        step::Receipt<'turn, 'd>,
    ) {
        let State {
            mut slots,
            dequeued,
            work,
            permit,
        } = self.state.into_parts().0;
        let (outcome, receipt) = {
            // SAFETY: selection owns this storage's occupied queue index through polling.
            let mut occupied = unsafe { raw::entry(slots.as_mut(), S::index(&dequeued)) };
            let fields = occupied.value().project();
            let target = *fields.target;
            let (parent, waker) = fields.binding.poll();
            let parent = wake::RootWaker::from(parent);
            let waker = wake::Waker::from_parent(waker);
            let ((result, wake), receipt) = step::Receipt::delegate(permit, |permit| {
                context::Context::with_waker(waker, parent, work, driver.reborrow(), |mut cx| {
                    let result = fields.mode.poll(context::PollCall::delegated(
                        fields.fiber,
                        cx.as_mut(),
                        permit,
                    ));
                    (result, cx.as_ref().get_ref().take_requeue())
                })
            });
            let outcome = match (result, wake) {
                (Poll::Pending, context::Requeue::Requested) => {
                    S::commit(dequeued, fields.ready.into_ref(), queue::Disposition::Ready);
                    Outcome::PendingReady { target }
                }
                (Poll::Pending, context::Requeue::Idle) => {
                    S::commit(dequeued, fields.ready.into_ref(), queue::Disposition::Idle);
                    Outcome::Pending { target }
                }
                (Poll::Ready(ControlFlow::Continue(output)), _) => {
                    S::commit(dequeued, fields.ready.into_ref(), queue::Disposition::Idle);
                    Outcome::Yielded { target, output }
                }
                (Poll::Ready(ControlFlow::Break(output)), _) => {
                    S::commit(
                        dequeued,
                        fields.ready.into_ref(),
                        queue::Disposition::Complete,
                    );
                    occupied.discard();
                    Outcome::Complete { target, output }
                }
            };
            (outcome, receipt)
        };
        (slots, outcome, receipt)
    }

    pub fn step(
        self,
        driver: &mut driver::Context<'_, 'd>,
    ) -> (Outcome<T, M::Output, M::Item>, step::Receipt<'turn, 'd>) {
        let (_, outcome, receipt) = self.drive_once(driver);
        (outcome, receipt)
    }
}

impl<'plane, 'turn, 'd, F, T: Copy, M: Mode<'d, F>> Item<'plane, 'turn, 'd, F, T, M> {
    pub fn drive(self, driver: &mut driver::Context<'_, 'd>) -> Outcome<T, M::Output, M::Item> {
        let queue = self.state.sink().dequeued.owner();
        let (slots, outcome, receipt) = self.drive_once(driver);
        if let Some(ready) = queue.snapshot()
            && let Some(index) = ready.peek()
        {
            ready.pause();
            // SAFETY: the returned slots and retained queue are this selection's original pair.
            let entry = unsafe { raw::entry(slots, index) }.into_value();
            wake::RootWaker::from(entry.as_ref().project_ref().binding.parent()).wake();
        }
        receipt.settle(step::Flow::Yield);
        outcome
    }
}

impl<'d, F, T: Copy, M, S: queue::Backend<'d>> Return for State<'_, '_, 'd, F, T, M, S> {
    type Item = ();

    fn return_item(self, (): ()) {
        let Self {
            slots,
            dequeued,
            permit,
            ..
        } = self;
        permit.run(|| {
            // SAFETY: abandonment retains the exact live storage/queue pair.
            let entry = unsafe { raw::entry(slots, S::index(&dequeued)) }.into_value();
            let fields = entry.into_ref().project_ref();
            S::commit(dequeued, fields.ready, queue::Disposition::Ready);
            wake::RootWaker::from(fields.binding.parent()).wake();
        });
    }
}

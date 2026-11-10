use std::pin;

use crate::{
    context::wake,
    core::driver::schedule::{app, ready::task::owner},
    task::storage::{
        fixed::schedule::{Mode, Storage, queue, ready::raw, result::Selection, selected},
        scope::Scope,
    },
};

impl<'d, F, D, T: Copy, M: Mode<'d, F>> Storage<'d, F, D, T, M> {
    pub fn select<'plane, 'turn>(
        self: pin::Pin<&'plane mut Self>,
        work: app::Work<'turn, 'd>,
    ) -> Selection<'plane, 'turn, 'd, F, T, M> {
        let storage = self.project();
        let mut slots = storage.slots;
        let queue = storage.queue.into_ref().queue();
        let Some(mut ready) = queue.snapshot() else {
            return Selection::Idle;
        };
        match work.next(&mut ready, &mut ()) {
            app::Next::Item(dequeued, permit) => {
                ready.pause();
                if let Some(index) = queue.slot().pop()
                    && index != dequeued.index
                {
                    // SAFETY: this queue only receives occupied indices from these slots.
                    let entry = unsafe { raw::entry(slots.as_mut(), index) }.into_value();
                    let fields = entry.into_ref().project_ref();
                    queue.merge(fields.ready);
                    wake::RootWaker::from(fields.binding.parent()).wake();
                }
                Selection::Ready(selected::Item::new(slots, dequeued, work, permit))
            }
            app::Next::Empty => Selection::Idle,
            app::Next::Exhausted(index) => {
                ready.pause();
                // SAFETY: the unchanged queue front retains its occupied storage admission.
                let entry = unsafe { raw::entry(slots, index) }.into_value();
                wake::RootWaker::from(entry.as_ref().project_ref().binding.parent()).wake();
                Selection::Yielded
            }
        }
    }
}

impl<'d, F, D, T: Copy, M: Mode<'d, F>, Source: Copy>
    Storage<'d, F, D, T, M, queue::Owned<'d, Source>>
{
    pub fn select<'plane, 'turn>(
        self: pin::Pin<&'plane mut Self>,
        scope: owner::Scope<Source>,
        work: app::Work<'turn, 'd>,
    ) -> Selection<'plane, 'turn, 'd, F, T, M, queue::Owned<'d, Source>> {
        let storage = self.project();
        let mut slots = storage.slots;
        let queue = storage.queue.into_ref().queue();
        loop {
            let ((dequeued, moved), permit) = match queue.next(scope, work) {
                app::Next::Item(item, permit) => (item, permit),
                app::Next::Empty => return Selection::Idle,
                app::Next::Exhausted(index) => {
                    // SAFETY: the queue retains this storage's occupied front.
                    let entry = unsafe { raw::entry(slots, index) }.into_value();
                    wake::RootWaker::from(entry.as_ref().project_ref().binding.parent()).wake();
                    return Selection::Yielded;
                }
            };
            if let Some(index) = moved {
                // SAFETY: ingress moved a live index within this storage's queue.
                let entry = unsafe { raw::entry(slots.as_mut(), index) }.into_value();
                wake::RootWaker::from(entry.as_ref().project_ref().binding.parent()).wake();
            }
            if let Some(dequeued) = dequeued {
                return Selection::Ready(selected::Item::new(slots, dequeued, work, permit));
            }
            permit.run(|| ());
        }
    }

    pub fn wake(self: pin::Pin<&mut Self>, scope: owner::Scope<Source>) {
        let _ = self.try_wake(scope);
    }

    pub fn try_wake(self: pin::Pin<&mut Self>, scope: owner::Scope<Source>) -> queue::Wake {
        let storage = self.project();
        let Some((index, wake)) = storage.queue.into_ref().queue().front(scope) else {
            return queue::Wake::Idle;
        };
        // SAFETY: the private queue front retains this storage's occupied task.
        let entry = unsafe { raw::entry(storage.slots, index) }.into_value();
        wake::RootWaker::from(entry.as_ref().project_ref().binding.parent()).wake();
        wake
    }
}

impl<'s, 'a, 'd, F, D, T: Copy, M: Mode<'d, F>>
    Scope<'s, pin::Pin<&'a mut Storage<'d, F, D, T, M>>>
{
    pub fn select<'plane, 'turn>(
        self: pin::Pin<&'plane mut Self>,
        work: app::Work<'turn, 'd>,
    ) -> Selection<'plane, 'turn, 'd, F, T, M> {
        self.get_mut().source.as_mut().select(work)
    }
}

impl<'s, 'a, 'd, F, D, T: Copy, M, Source>
    Scope<'s, pin::Pin<&'a mut Storage<'d, F, D, T, M, queue::Owned<'d, Source>>>>
{
    pub fn owners(self: pin::Pin<&Self>) -> queue::Owners<'_, 'd, Source> {
        self.get_ref().source.as_ref().owners()
    }
}

impl<'s, 'a, 'd, F, D, T: Copy, M: Mode<'d, F>, Source: Copy>
    Scope<'s, pin::Pin<&'a mut Storage<'d, F, D, T, M, queue::Owned<'d, Source>>>>
{
    pub fn select<'plane, 'turn>(
        self: pin::Pin<&'plane mut Self>,
        scope: owner::Scope<Source>,
        work: app::Work<'turn, 'd>,
    ) -> Selection<'plane, 'turn, 'd, F, T, M, queue::Owned<'d, Source>> {
        self.get_mut().source.as_mut().select(scope, work)
    }

    pub fn wake(self: pin::Pin<&mut Self>, scope: owner::Scope<Source>) {
        self.get_mut().source.as_mut().wake(scope);
    }

    pub fn try_wake(self: pin::Pin<&mut Self>, scope: owner::Scope<Source>) -> queue::Wake {
        self.get_mut().source.as_mut().try_wake(scope)
    }
}

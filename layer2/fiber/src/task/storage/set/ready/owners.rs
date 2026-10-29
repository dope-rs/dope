use std::{pin, task};

use o3::mem::quota;

use crate::{
    abi, context,
    core::driver::{
        retained,
        schedule::{app, ready::task::owner},
    },
    task::storage::{
        Handle,
        fixed::schedule::{
            queue,
            ready::{Global, owned},
        },
        set::{
            self,
            entries::Entries,
            ready::{self, core},
            slot,
            slot::state::State,
            views,
        },
    },
};

#[derive(Clone, Copy)]
pub(in crate::task::storage::set) struct Source;

#[derive(Clone, Copy)]
pub(in crate::task::storage::set) struct Identity(owner::Lane, owner::Generation);

impl owner::Identity for Identity {
    fn parts(self) -> (owner::Lane, owner::Generation) {
        (self.0, self.1)
    }
}

impl owner::Source for Source {
    type Identity = Identity;
}

pub(in crate::task::storage::set) fn owner(key: impl owner::Identity) -> owner::Owner<Source> {
    let parts = key.parts();
    owner::Owner::from(Identity(parts.0, parts.1))
}

pub(in crate::task::storage::set) fn error(error: queue::OwnerError<Source>) -> owner::Error {
    match error {
        queue::OwnerError::Foreign { owner } => owner::Error::Foreign {
            lane: owner.lane(),
            generation: owner.generation(),
        },
        queue::OwnerError::Stale { owner, current } => owner::Error::Stale {
            lane: owner.lane(),
            generation: owner.generation(),
            current,
        },
    }
}

fn index<D>(global: Global) -> ready::raw::Index<D> {
    // SAFETY: Global retains a non-sentinel index admitted to the same task extent.
    unsafe { ready::raw::Index::from_offset(global.get() as u32) }
}

pub(in crate::task::storage::set) fn wake<'d, F: abi::Fiber<'d>, M, D, G>(
    queue: pin::Pin<&owned::Queue<'d, Source>>,
    view: &views::View<'_, 'd, F, M, D, G>,
) {
    if let Some((global, _)) = queue.front(owner::Scope::Any) {
        // SAFETY: queue membership or ingress retains this exact live root.
        unsafe { slot::raw::Queued::new(view.scheduled(index(global))) }.wake();
    }
}

pub(in crate::task::storage::set) fn drive<'d, F: abi::Fiber<'d>, M, D, G>(
    queue: pin::Pin<&owned::Queue<'d, Source>>,
    entries: Entries<'_, 'd, F, M, D, G>,
    scope: owner::Scope<Source>,
    work: app::Work<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
    mut observe: impl FnMut(&M, pin::Pin<&mut F>),
) -> set::Length {
    let (view, mut token) = entries.into_parts();
    for entry in (0..work.remaining().saturating_add(1)).map_while(|_| queue.sync()) {
        view.scheduled(index(entry.index())).queue_wake();
    }
    let mut done = 0;
    while let quota::Admission::Item((dequeued, permit)) = work.claim(|| queue.dequeue(scope)) {
        let mut running = core::Core::running(&view, &mut token, index(dequeued.index()));
        let (parent, waker) = running.wakers();
        let poll =
            context::Context::with_waker(waker, parent, work, driver.reborrow(), |mut context| {
                running.poll_with(|mut fiber, data| {
                    // SAFETY: the selected owner retains this live pinned slot. A
                    // Ready result consumes Running and retires its fiber below.
                    let result = unsafe {
                        context::raw::Live::new(fiber.as_mut()).with(context.as_mut(), permit)
                    };
                    observe(data, fiber);
                    result
                })
            });
        match poll {
            task::Poll::Pending => dequeued.idle(),
            task::Poll::Ready(output) => {
                running.complete(output);
                dequeued.detach();
                done += 1;
            }
        }
    }
    wake(queue, &view);
    set::Length::new(done)
}

pub(in crate::task::storage::set) fn remove<'a, 'd, F: abi::Fiber<'d>, M, D, G>(
    queue: pin::Pin<&owned::Queue<'d, Source>>,
    view: &views::View<'a, 'd, F, M, D, G>,
    token: &mut slot::Token<'a, F, M>,
    key: Handle<'d, D>,
) -> Option<State> {
    let removal = core::Removal::resolve(view, token, key)?;
    if let Some(index) = removal.runnable() {
        // SAFETY: this current slot and exclusive token retain its exact root's
        // admission to the paired owner queue, excluding recycling and polling.
        unsafe { owned::raw::detach(queue, Global::new(index.position())) };
    }
    Some(removal.remove(core::Kind::Grouped))
}

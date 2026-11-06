use dope_core::driver::schedule::clock;
use o3::depot::{arena::slab::key, capacity, queue::round::set};

use crate::service::{
    health,
    pool::members::{Idle, Member, Occupied, Ready, Target, Unlinked, queues},
};

/// One admitted failure owns the unlinked resident with its current canonical health.
pub(super) struct Failure<'a, 'h, I, A> {
    entry: Unlinked<'a, 'h, I, A>,
}

impl<'a, 'h, I, A> Failure<'a, 'h, I, A> {
    /// Admits a desired member after its caller settled the exact exclusive resident.
    pub(super) fn try_new(entry: Occupied<'a, 'h, I, A>, target: Target<'h>) -> Option<Self> {
        if !entry.get().health.matches(target) {
            return None;
        }
        let entry = match entry.state() {
            set::State::Ready(entry) => {
                if entry.get().health.ready == Ready::Probe {
                    return None;
                }
                entry.remove()
            }
            set::State::Idle(entry) => match entry.get().health.idle {
                Idle::Probing => entry,
                Idle::Backoff | Idle::Exhausted => return None,
            },
        };
        Some(Self { entry })
    }

    pub(super) fn apply<'d>(
        mut self,
        queue: &mut queues::Queue<'h, 'd>,
        backoff: &mut health::Backoff,
        now: clock::Moment<'d>,
    ) {
        let retry_at = backoff.retry_at(&mut self.entry.get_mut().health.failures, now);
        queue.backoff(self, retry_at);
    }

    pub(super) fn key(&self) -> capacity::Key<'h, key::Handle<Member>> {
        self.entry.key()
    }

    pub(super) fn commit(mut self) {
        self.entry.get_mut().health.idle = Idle::Backoff;
    }
}

use dope_core::driver::{
    route::{Identity, KeyTag},
    schedule::maint,
};
use o3::depot::arena::slab::external::Generation as _;

use crate::link::pool::{
    self,
    pending::{Chain, Detached, Disposition, Lock, Next, Queue, State, Status, Topology},
};

/// Owns the initial pending chain; later wakes belong to the next round.
#[must_use = "dropping a round restores its unprocessed entries"]
pub struct Round<'p, 'd, ID: Identity = KeyTag<0>> {
    queue: &'p Queue<'d, ID>,
    keys: pool::Keyspace<'d, ID>,
    topology: Topology<'d, ID>,
}

impl<'p, 'd, ID: Identity> Round<'p, 'd, ID> {
    pub(super) fn take(
        queue: &'p Queue<'d, ID>,
        keys: pool::Keyspace<'d, ID>,
        topology: Topology<'d, ID>,
    ) -> Self {
        queue.status.set(Status::Draining);
        queue.topology.set(Topology::Empty);
        Self { queue, keys, topology }
    }

    /// Runs only the original pending chain, spending one maintenance permit per head.
    pub fn run(
        mut self,
        work: maint::Work<'_, 'd>,
        mut run: impl FnMut(pool::Key<'d, ID>) -> Disposition,
    ) {
        while let Topology::Occupied(Chain { head, tail }) = self.topology
            && let Some(permit) = work.permit()
        {
            permit.run(|| {
                let queue = self.queue;
                let node = queue.node(head);
                let key = self.keys.bind_parts(head.lane(), node.epoch.get());
                self.topology = match node.next.replace(Next::End) {
                    Next::End => Topology::Empty,
                    Next::Slot(head) => Topology::Occupied(Chain { head, tail }),
                };
                node.state.set(State::Claimed);
                queue.claimed.set(Lock::Held(head));
                let outcome = run(key);
                queue.end_capacity(head);
                queue.claimed.set(Lock::Free);
                match outcome {
                    Disposition::Gone => {
                        node.state.set(State::Idle);
                        queue.clear_wait(head);
                        match key.epoch().next() {
                            Some(epoch) => node.epoch.set(epoch),
                            None => node.state.set(State::Retired),
                        }
                    }
                    Disposition::Idle => {
                        if matches!(node.state.replace(State::Idle), State::Awakened) {
                            queue.enqueue(Detached { index: head, node });
                        }
                    }
                }
            });
        }
    }
}

impl<ID: Identity> Drop for Round<'_, '_, ID> {
    fn drop(&mut self) {
        if let Topology::Occupied(mut chain) = self.topology {
            if let Topology::Occupied(next) = self.queue.topology.get() {
                self.queue.node(chain.tail).next.set(Next::Slot(next.head));
                chain.tail = next.tail;
            }
            self.queue.topology.set(Topology::Occupied(chain));
        }
        self.queue.status.set(Status::Ready);
    }
}

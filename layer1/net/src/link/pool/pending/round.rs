use std::mem;

use dope_core::driver::{
    route::{Identity, KeyTag},
    schedule::maint,
};
use o3::{depot::arena::slab::external::Generation as _, mem::quota};

use crate::link::pool::{
    self,
    pending::{
        self, Busy, Chain, Detached, Disposition, Lock, Next, Queue, State, Status, Topology, Wake,
    },
};

/// Owns the initial pending chain; later wakes belong to the next round.
#[must_use = "dropping a round restores its unprocessed entries"]
pub struct Round<'p, 'd, ID: Identity = KeyTag<0>> {
    queue: &'p Queue<'d, ID>,
    keys: pool::Keyspace<'d, ID>,
    topology: Topology<'d, ID>,
}

/// One head exclusively borrowed from its owning round.
struct Front<'a, 'p, 'd, ID: Identity> {
    round: &'a mut Round<'p, 'd, ID>,
    chain: Chain<'d, ID>,
}

impl<'p, 'd, ID: Identity> Round<'p, 'd, ID> {
    pub(super) fn try_new(
        queue: &'p Queue<'d, ID>,
        keys: pool::Keyspace<'d, ID>,
        capacity: bool,
    ) -> Result<Self, Busy> {
        if matches!(queue.status.get(), Status::Draining) {
            return Err(Busy);
        }
        if capacity {
            queue.release_send();
        }
        queue.status.set(Status::Draining);
        Ok(Self {
            queue,
            keys,
            topology: queue.topology.replace(Topology::Empty),
        })
    }

    /// Runs only the original pending chain, spending one maintenance permit per head.
    pub fn run(
        mut self,
        work: maint::Work<'_, 'd>,
        mut run: impl FnMut(pool::Key<'d, ID>) -> Disposition,
    ) {
        loop {
            let admitted = work.claim(|| self.head());
            let (front, permit) = match admitted {
                quota::Admission::Item(item) => item,
                quota::Admission::Empty | quota::Admission::Exhausted => break,
            };
            permit.run(|| front.run(&mut run));
        }
    }

    fn head(&mut self) -> Option<Front<'_, 'p, 'd, ID>> {
        match self.topology {
            Topology::Empty => None,
            Topology::Occupied(chain) => Some(Front { round: self, chain }),
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

impl<'d, ID: Identity> Front<'_, '_, 'd, ID> {
    /// Runs one exact head; its claim cannot escape or be resolved on another queue.
    fn run(self, run: impl FnOnce(pool::Key<'d, ID>) -> pending::Disposition) {
        let Self {
            round,
            chain: Chain { head, tail },
        } = self;
        let queue = round.queue;
        let node = queue.node(head);
        let key = round.keys.bind_parts(head.lane(), node.epoch.get());
        round.topology = match node.next.replace(Next::End) {
            Next::End => Topology::Empty,
            Next::Slot(head) => Topology::Occupied(Chain { head, tail }),
        };
        node.wake.set(Wake::Quiet);
        node.state.set(State::Claimed);
        queue.claimed.set(Lock::Held(head));
        let outcome = run(key);
        queue.end_capacity(head);
        node.state.set(State::Idle);
        queue.claimed.set(Lock::Free);
        match outcome {
            pending::Disposition::Gone => {
                queue.clear_wait(head);
                match key.epoch().next() {
                    Some(epoch) => node.epoch.set(epoch),
                    None => node.state.set(State::Retired),
                }
            }
            pending::Disposition::Idle => match node.wake.get() {
                Wake::Quiet => {}
                Wake::Pending => queue.enqueue(Detached { index: head, node }),
            },
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<Front<'static, 'static, 'static, KeyTag<0>>>()
            == 2 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Option<Front<'static, 'static, 'static, KeyTag<0>>>>()
            == mem::size_of::<Front<'static, 'static, 'static, KeyTag<0>>>()
    );
};

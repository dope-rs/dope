use dope_net::link::egress::{data, metadata::arena};
use o3::cell::region;

use crate::{
    connector::{app, port::state},
    dispatch::typed::identity,
};

struct Suspension<'a, 'd, I: identity::Identity> {
    entry: &'a state::Entry<'d, I>,
    transaction: state::Transaction<I>,
    armed: bool,
}

impl<'a, 'd, I: identity::Identity> Suspension<'a, 'd, I> {
    fn begin(entry: &'a state::Entry<'d, I>, transaction: state::Transaction<I>) -> Option<Self> {
        entry.suspend(transaction).then_some(Self {
            entry,
            transaction,
            armed: true,
        })
    }

    fn restore(mut self) -> bool {
        self.armed = false;
        self.entry.restore(self.transaction)
    }
}

impl<I: identity::Identity> Drop for Suspension<'_, '_, I> {
    fn drop(&mut self) {
        if self.armed {
            self.entry.restore(self.transaction);
        }
    }
}

pub(super) struct Receiver<'a, 'd, B, I: identity::Identity> {
    slot: arena::Slot<'a, 'd, B, state::Entry<'d, I>>,
    transaction: state::Transaction<I>,
}

impl<'a, 'd, B: data::Payload<'d>, I: identity::Identity> Receiver<'a, 'd, B, I> {
    pub(super) const fn new(
        slot: arena::Slot<'a, 'd, B, state::Entry<'d, I>>,
        transaction: state::Transaction<I>,
    ) -> Self {
        Self { slot, transaction }
    }

    pub(super) fn drain(
        &self,
        token: &mut region::Token<'d>,
        drain: &mut app::RequestDrain<'_, 'd, B>,
        mut begin: impl FnMut(),
    ) {
        let entry = self.slot.state();
        let Some(suspension) = Suspension::begin(entry, self.transaction) else {
            return;
        };
        loop {
            let queue = self.slot.queue();
            let ((value, mut front), permit) = match drain.admit(queue.front(token)) {
                app::RequestAdmission::Item(item, permit) => (item, permit),
                app::RequestAdmission::Empty | app::RequestAdmission::Exhausted => {
                    suspension.restore();
                    return;
                }
            };
            match front.with_region(|region| permit.try_push(region, value)) {
                Ok(()) => {
                    front.release();
                    begin();
                    if !entry.is_suspended(self.transaction) {
                        return;
                    }
                }
                Err(value) if entry.is_suspended(self.transaction) => {
                    drop(front.try_restore(value));
                    suspension.restore();
                    return;
                }
                Err(value) => {
                    front.release();
                    drop(value);
                    return;
                }
            }
        }
    }

    pub(super) fn take_close(&self) -> bool {
        self.slot.state().take_close(self.transaction)
    }
}

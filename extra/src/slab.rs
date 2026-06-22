use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;

use dope::Driver;
use dope::runtime::park;
use dope::runtime::park::Parker;
use dope::runtime::token::{Epoch, LocalIdx, ROUTE_TASKS, Token};

type Task = Pin<Box<dyn Future<Output = ()>>>;

struct TaskSlot {
    fut: RefCell<Option<Task>>,
    epoch: std::cell::Cell<Epoch>,
}

pub(super) struct Slab {
    slots: Box<[TaskSlot]>,
    free: RefCell<Vec<u32>>,
    park_slots: Box<[park::Slot]>,
}

impl Slab {
    pub(super) fn with_capacity(capacity: usize, driver: &Driver) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        let mut free = Vec::with_capacity(capacity);
        for i in 0..capacity {
            slots.push(TaskSlot {
                fut: RefCell::new(None),
                epoch: std::cell::Cell::new(Epoch::ZERO),
            });
            free.push((capacity - 1 - i) as u32);
        }
        let park_slots: Vec<park::Slot> = (0..capacity)
            .map(|i| {
                Parker::make_slot(
                    driver,
                    Token::new(ROUTE_TASKS, LocalIdx::new(i as u32), Epoch::INITIAL),
                )
            })
            .collect();
        Self {
            slots: slots.into_boxed_slice(),
            free: RefCell::new(free),
            park_slots: park_slots.into_boxed_slice(),
        }
    }

    pub(super) fn try_spawn<F>(&mut self, fut: F) -> Option<()>
    where
        F: Future<Output = ()> + 'static,
    {
        let idx = self.free.borrow_mut().pop()?;
        self.place(idx, Box::pin(fut));
        Some(())
    }

    fn place(&mut self, idx: u32, fut: Task) {
        let slot = &self.slots[idx as usize];
        let new_epoch = slot.epoch.get().bump();
        slot.epoch.set(new_epoch);
        *slot.fut.borrow_mut() = Some(fut);
        let id = self.slot_id(idx, new_epoch);
        let slot_ref = &self.park_slots[idx as usize];
        slot_ref.set_target(id);
        slot_ref.wake();
    }

    fn slot_id(&self, idx: u32, epoch: Epoch) -> Token {
        let epoch = if epoch == Epoch::ZERO {
            Epoch::INITIAL
        } else {
            epoch
        };
        Token::new(ROUTE_TASKS, LocalIdx::new(idx), epoch)
    }

    fn make_waker(&self, idx: u32, epoch: Epoch) -> std::task::Waker {
        let id = self.slot_id(idx, epoch);
        let slot_ref = &self.park_slots[idx as usize];
        slot_ref.set_target(id);
        slot_ref.make_waker()
    }

    pub(super) fn wake_slot(&self, local: LocalIdx) {
        if (local.raw() as usize) >= self.slots.len() {
            return;
        }
        self.poll_slot(local.raw());
    }

    fn poll_slot(&self, idx: u32) {
        let slot = &self.slots[idx as usize];
        let epoch = slot.epoch.get();
        let waker = self.make_waker(idx, epoch);
        let mut cx = Context::from_waker(&waker);
        let mut fut_borrow = slot.fut.borrow_mut();
        let Some(fut) = fut_borrow.as_mut() else {
            return;
        };
        if fut.as_mut().poll(&mut cx).is_ready() {
            *fut_borrow = None;
            drop(fut_borrow);
            self.free.borrow_mut().push(idx);
        }
    }
}

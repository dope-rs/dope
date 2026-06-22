use std::ptr::NonNull;

use super::Driver;
use crate::backend::park::{Parker, Slot};
use crate::backend::token::Token;
use crate::socket::FdSlot;

impl Parker for Driver {
    fn slot(&self, slot: FdSlot) -> &crate::backend::park::Slot {
        self.arena.slot(slot)
    }

    fn make_slot(&self, target: Token) -> crate::backend::park::Slot {
        Slot::new(target, NonNull::from(&*self.arena))
    }

    fn drain(&self, out: &mut Vec<Token>) {
        self.arena.drain(out);
    }

    fn is_empty(&self) -> bool {
        self.arena.is_empty()
    }
}

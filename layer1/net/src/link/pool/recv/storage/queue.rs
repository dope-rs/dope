use o3::depot::{arena::linked::slots, queue::pool};
use pool::Front;

use crate::link::event;

pub type Vacant<'a, 'd> =
    slots::Vacant<event::Completion<'d>, slots::Bound<'a, event::Completion<'d>>>;

pub trait Ops<'d> {
    fn prepare_pop(&mut self) -> Option<Front<'_, event::Completion<'d>, ()>>;
    fn has_queued(&self) -> bool;
}

pub trait Write<'d>: Ops<'d> {
    fn prepare_push(&mut self) -> Option<Vacant<'_, 'd>>;
}

impl<'d> Ops<'d> for &mut () {
    fn prepare_pop(&mut self) -> Option<Front<'_, event::Completion<'d>, ()>> {
        None
    }
    fn has_queued(&self) -> bool {
        false
    }
}

impl<'d> Ops<'d> for pool::Lane<'_, event::Completion<'d>> {
    fn prepare_pop(&mut self) -> Option<Front<'_, event::Completion<'d>, ()>> {
        self.front_mut()
    }
    fn has_queued(&self) -> bool {
        !self.is_empty()
    }
}

impl<'d> Write<'d> for pool::Lane<'_, event::Completion<'d>> {
    fn prepare_push(&mut self) -> Option<Vacant<'_, 'd>> {
        self.vacant()
    }
}

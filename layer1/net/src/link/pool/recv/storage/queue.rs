use o3::depot::{arena::linked::slots, queue::pool};
use pool::Front;

use crate::link::event;

pub type Vacant<'a, 'd, R = ()> =
    slots::Vacant<event::Completion<'d, R>, slots::Bound<'a, event::Completion<'d, R>>>;

pub trait Ops<'d, R = ()> {
    fn prepare_pop(&mut self) -> Option<Front<'_, event::Completion<'d, R>, ()>>;
    fn has_queued(&self) -> bool;
}

pub trait Write<'d, R = ()>: Ops<'d, R> {
    fn prepare_push(&mut self) -> Option<Vacant<'_, 'd, R>>;
}

impl<'d, R> Ops<'d, R> for &mut () {
    fn prepare_pop(&mut self) -> Option<Front<'_, event::Completion<'d, R>, ()>> {
        None
    }
    fn has_queued(&self) -> bool {
        false
    }
}

impl<'d, R> Ops<'d, R> for pool::Lane<'_, event::Completion<'d, R>> {
    fn prepare_pop(&mut self) -> Option<Front<'_, event::Completion<'d, R>, ()>> {
        self.front_mut()
    }
    fn has_queued(&self) -> bool {
        !self.is_empty()
    }
}

impl<'d, R> Write<'d, R> for pool::Lane<'_, event::Completion<'d, R>> {
    fn prepare_push(&mut self) -> Option<Vacant<'_, 'd, R>> {
        self.vacant()
    }
}

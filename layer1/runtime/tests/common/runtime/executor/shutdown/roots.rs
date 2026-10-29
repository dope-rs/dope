use std::{cell::Cell, rc::Rc, task};

use dope_runtime::executor::root;

pub struct Dropped {
    pub dropped: Rc<Cell<bool>>,
    pub valid: Rc<Cell<bool>>,
}

impl Drop for Dropped {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}

impl<'d> root::Root<'d> for Dropped {
    type Output = ();

    fn poll(ctx: root::Context<'_, 'd, Self>) -> task::Poll<Self::Output> {
        let (root, _, _, _) = ctx.into_parts();
        let this = root.as_ref().get_ref();
        this.valid.set(this.valid.get() && !this.dropped.get());
        task::Poll::Pending
    }
}

pub struct ReadyDrop(pub Rc<Cell<bool>>);

impl Drop for ReadyDrop {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

impl<'d> root::Root<'d> for ReadyDrop {
    type Output = bool;

    fn poll(ctx: root::Context<'_, 'd, Self>) -> task::Poll<Self::Output> {
        let (root, _, _, _) = ctx.into_parts();
        task::Poll::Ready(!root.as_ref().get_ref().0.get())
    }
}

pub struct Outcome<'a> {
    pub output: task::Poll<Result<(), u8>>,
    pub polls: &'a Cell<Option<usize>>,
    pub dropped: &'a Cell<bool>,
    pub begun: &'a Cell<bool>,
}

impl Drop for Outcome<'_> {
    fn drop(&mut self) {
        self.dropped.set(!self.begun.get());
    }
}

impl<'d> root::Root<'d> for Outcome<'_> {
    type Output = Result<(), u8>;

    fn poll(ctx: root::Context<'_, 'd, Self>) -> task::Poll<Self::Output> {
        let (root, _, _, _) = ctx.into_parts();
        let this = root.as_ref().get_ref();
        this.polls
            .set(this.polls.get().and_then(|polls| polls.checked_add(1)));
        this.output
    }
}

use core::pin;

use crate::{
    abi,
    task::storage::{
        Handle,
        set::{Allocation, ready, reserves, slot, slot::state::State, views},
    },
};

#[repr(transparent)]
pub(super) struct Entries<'a, 'd, F: abi::Fiber<'d>, M, D, G = ()> {
    view: views::View<'a, 'd, F, M, D, G>,
    token: slot::Token<'a, F, M>,
}

impl<'a, 'd, F: abi::Fiber<'d>, M, D, G> Entries<'a, 'd, F, M, D, G> {
    pub(super) fn new(pool: &'a mut Allocation<'d, F, M, D, G>) -> Self {
        Self {
            view: views::View::new(pool),
            token: slot::Token::new(),
        }
    }

    pub(super) fn view(&self) -> views::View<'a, 'd, F, M, D, G> {
        self.view.reborrow()
    }

    pub(super) fn vacant(&mut self) -> Option<reserves::Entry<'_, 'a, 'd, F, M, D, G>> {
        let vacant = self.view.vacant()?;
        Some(reserves::Entry::new(
            self.view.reborrow(),
            &mut self.token,
            vacant,
        ))
    }

    pub(super) fn resident<R>(
        &mut self,
        key: Handle<'d, D>,
        access: impl FnOnce(pin::Pin<&mut F>) -> R,
    ) -> Option<R> {
        let entry = self.view.resolve(key)?;
        Some(access(entry.resident(&mut self.token)?))
    }

    pub(super) fn remove(&mut self, key: Handle<'d, D>) -> Option<State> {
        ready::Queue::remove(&self.view, &mut self.token, key)
    }

    pub(super) fn clear(&mut self) {
        self.view.clear(&mut self.token);
    }

    pub(super) fn into_parts(self) -> (views::View<'a, 'd, F, M, D, G>, slot::Token<'a, F, M>) {
        (self.view, self.token)
    }
}

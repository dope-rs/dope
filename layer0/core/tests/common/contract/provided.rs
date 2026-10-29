use std::{cell, mem};

use dope_core::driver::{life, storage};
use o3::{
    buffer::storage::{
        provided,
        provided::{account, view},
    },
    mem::rc,
};

pub struct Item<'a> {
    bytes: [u8; 4],
    drops: &'a cell::Cell<usize>,
}

impl Drop for Item<'_> {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

pub struct Joined<'s>(pub &'s cell::Cell<usize>);

type Arenas<'s> = (provided::Arena<Item<'s>>, provided::Arena<Item<'s>>);

impl<'s> life::Operation<storage::Scope<Arenas<'s>>, ()> for Joined<'s> {
    type Output = Result<(usize, usize), Error>;

    fn run<'a, 'p, 'd>(
        self,
        mut scope: life::Scope<'a, 'd>,
        storage: storage::Loan<'a, 'p, 'd, storage::Scope<Arenas<'s>>>,
        _: &'a (),
    ) -> Self::Output
    where
        'd: 'p,
        'p: 'a,
        'd: 'a,
        Self: 'd,
        storage::Scope<Arenas<'s>>: 'd,
    {
        let (first, second) = *storage.pinned().get_ref();
        let context = scope.context();
        let budget = context
            .storage
            .allocator()
            .try_budget(8)
            .map_err(Error::Budget)?;
        for root in [first, second] {
            let vacant = root.vacant().ok_or(Error::Full)?;
            let owner = vacant.try_insert(
                Item {
                    bytes: *b"held",
                    drops: self.0,
                },
                4,
                |item| &mut item.bytes,
            );
            let owner = owner.map_err(|error| Error::Projection(error.discard()))?;
            let held = owner
                .try_account(&budget)
                .map_err(|error| Error::Account(error.discard()))?;
            let alias = held.try_clone().map_err(Error::Clone)?;
            mem::forget(held);
            mem::forget(alias);
        }
        Ok((budget.resident(), self.0.get()))
    }
}

#[derive(Debug)]
pub enum Error {
    Budget(storage::Exhausted),
    Full,
    Projection(view::LengthError),
    Account(account::Cause),
    Clone(rc::Error),
}

use core::pin;

use set::{insert, ready, ready::owners, slot};

use crate::{
    abi,
    context::wake,
    core::driver::schedule::ready::{task, task::owner},
    task::{
        storage,
        storage::{
            fixed::schedule::ready::{Global, owned},
            set,
            set::views,
        },
    },
};

#[must_use]
#[repr(C)]
pub(super) struct Entry<'e, 'a, 'd, F: abi::Fiber<'d>, M, D, G = ()> {
    view: views::View<'a, 'd, F, M, D, G>,
    token: &'e mut slot::Token<'a, F, M>,
    vacant: views::Vacant<'a, 'd, F, M, D, G>,
}

impl<'e, 'a, 'd, F: abi::Fiber<'d>, M, D, G> Entry<'e, 'a, 'd, F, M, D, G> {
    pub(in crate::task::storage::set) fn new(
        view: views::View<'a, 'd, F, M, D, G>,
        token: &'e mut slot::Token<'a, F, M>,
        vacant: views::Vacant<'a, 'd, F, M, D, G>,
    ) -> Self {
        Self {
            view,
            token,
            vacant,
        }
    }

    pub(in crate::task::storage::set) fn try_insert(
        self,
        fiber: F,
        data: M,
        parent: wake::RootWaker<'d>,
    ) -> Result<storage::Id<'d, D>, insert::Error> {
        use insert::Error;

        let index = self.vacant.index();
        let wake: wake::Waker<'d> = parent.into();
        let admit = wake.0.try_admit().map_err(Error::Admission)?;
        let attach = ready::Queue::try_attach(self.view.reborrow(), index, parent)
            .map_err(Error::Topology)?;
        let root = unsafe { task::raw::Root::bind(admit, attach) };
        Ok(self.finish(root, fiber, data, parent, None))
    }

    pub(in crate::task::storage::set) fn try_owned(
        self,
        fiber: F,
        data: M,
        parent: wake::RootWaker<'d>,
        queue: pin::Pin<&owned::Queue<'d, owners::Source>>,
        owner: owner::Owner<owners::Source>,
    ) -> Result<storage::Id<'d, D>, insert::Error> {
        let owner = queue
            .rebind(owner)
            .map_err(owners::error)
            .map_err(insert::Error::Owner)?;
        let index = Global::new(self.vacant.index().position());
        let wake: wake::Waker<'d> = parent.into();
        let admit = wake.0.try_admit().map_err(insert::Error::Admission)?;
        let (edge, prepared) = owner
            .try_prepare(index, wake.0)
            .map_err(insert::Error::Topology)?;
        // SAFETY: this admission and edge use the same retained parent.
        let root = unsafe { task::raw::Root::bind(admit, edge) };
        Ok(self.finish(root, fiber, data, parent, Some(prepared)))
    }

    fn finish(
        self,
        root: task::raw::Root<'d>,
        fiber: F,
        data: M,
        parent: wake::RootWaker<'d>,
        owned: Option<owned::Prepared<'_, 'd, owners::Source>>,
    ) -> storage::Id<'d, D> {
        let Self {
            view,
            token,
            vacant,
        } = self;
        let (slot, key) = vacant.acquire();
        let admitted = slot.write(token, root, fiber, data);
        let admitted = view.header().admit(admitted);
        match owned {
            Some(prepared) => {
                admitted.queue();
                prepared.attach().enqueue();
            }
            None => ready::Queue::insert(&view, token, admitted, key),
        }
        parent.wake();
        let owner = storage::Owner::of(view.header());
        storage::Id::from_key(key, owner)
    }
}

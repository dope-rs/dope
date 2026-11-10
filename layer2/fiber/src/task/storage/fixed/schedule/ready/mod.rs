use std::num::NonZero;

use o3::depot::queue::links;

use crate::{
    context::wake,
    core::driver::schedule::ready::task::{self, Error, Parent, topology},
};

pub mod linked;
pub mod owned;
pub(super) mod raw;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Global(links::Index);

impl Global {
    pub(in crate::task::storage) fn new(position: NonZero<u32>) -> Self {
        Self(position.into())
    }

    pub(in crate::task::storage) fn from_slot(index: u32) -> Self {
        // SAFETY: an admitted native slot is below its u32 capacity.
        Self::new(unsafe { NonZero::new_unchecked(index + 1) })
    }

    pub(in crate::task::storage) const fn get(self) -> usize {
        self.0.get()
    }
}

pub(super) fn try_bind<'a, 'd, I, P, E>(
    parent: wake::RootWaker<'d>,
    prepare: impl FnOnce(Parent<'d>) -> Result<(topology::Edge<'a, 'd, I>, P), E>,
) -> Result<(task::raw::Root<'d>, P), E>
where
    'd: 'a,
    I: topology::raw::Index + 'd,
    E: From<Error>,
{
    let parent: wake::Waker<'d> = parent.into();
    let admit = parent.0.try_admit().map_err(E::from)?;
    let (edge, prepared) = prepare(parent.0)?;
    let binding = unsafe { task::raw::Root::bind(admit, edge) };
    Ok((binding, prepared))
}

const _: () = {
    assert!(std::mem::size_of::<Global>() == std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<Option<Global>>() == std::mem::size_of::<Global>());
};

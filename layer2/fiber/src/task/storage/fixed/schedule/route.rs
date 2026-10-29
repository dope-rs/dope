use std::{marker, num, pin};

use crate::{
    context::wake,
    core::driver::schedule::ready::task,
    task::storage::fixed::{
        local,
        quota::{self, State},
        schedule::{self, queue, ready::Path},
        tree,
    },
};

pub struct Route<'a, 'd, R, P, Q = quota::Physical, S = queue::Flat<'d>>
where
    R: tree::At<'d, P>,
{
    pub(super) plane: pin::Pin<&'a mut schedule::Plane<'d, R, Q, S>>,
    pub(super) path: marker::PhantomData<fn() -> P>,
}

impl<'a, 'd, R, P, Q, S, M> Route<'a, 'd, R, P, Q, S>
where
    R: tree::At<'d, P, Member = M>,
    M: schedule::Ops<'d> + 'a,
    Q: State,
    S: queue::Backend<'d>,
    'd: 'a,
{
    pub fn try_admit(
        &mut self,
        input: S::Input,
        parent: wake::RootWaker<'d>,
    ) -> Result<impl FnOnce(schedule::Input<'d, M>) -> schedule::Identity<'d, M>, S::Error> {
        let (routes, quota, queue) = self.plane.as_mut().split();
        let Some(vacancy) = Q::vacancy(*quota) else {
            return Err(S::Error::from(task::Error::Capacity {
                requested: num::NonZero::<usize>::MIN,
                available: 0,
            }));
        };
        let path = Path::<R, P>::new(R::project(routes));
        let insert = schedule::Ops::try_admit(path, queue, input, parent)?;
        Ok(move |(fiber, target)| {
            let id = insert((fiber, target));
            *quota = Q::consume(*quota, vacancy);
            parent.wake();
            id
        })
    }

    pub fn remove(&mut self, id: schedule::Identity<'d, M>) -> local::Removal {
        let (routes, quota, queue) = self.plane.as_mut().split();
        let path = Path::<R, P>::new(R::project(routes));
        let removed = schedule::Ops::remove(path, id, queue);
        if removed == local::Removal::Removed {
            *quota = State::retire(*quota, quota::Retired::new());
        }
        removed
    }
}

impl<'a, 'd, R, P: 'a> Route<'a, 'd, R, P>
where
    R: tree::At<'d, P>,
    R::Member: schedule::Ops<'d>,
    'd: 'a,
{
    /// Reserves insertion without a cancellation identity.
    pub fn try_spawn(
        self,
        parent: wake::RootWaker<'d>,
    ) -> Result<impl FnOnce(schedule::Input<'d, R::Member>) + 'a, task::Error> {
        let (routes, &mut quota::Physical, queue) = self.plane.split();
        let path = Path::<R, P>::new(R::project(routes));
        let insert = schedule::Ops::try_spawn(path, queue, parent)?;
        Ok(move |(fiber, target)| {
            insert((fiber, target));
            parent.wake();
        })
    }
}

use std::pin;

use o3::{
    base::cell::{brand, region},
    buffer,
    buffer::storage::provided::{self, fixed},
};

use crate::{
    Engine,
    driver::{
        self,
        life::{Operation, Root, domain, epochs::Closed},
        storage,
    },
};

pub(super) struct Provided<O>(pub(super) O);

impl<A, Q: ?Sized, O> fixed::Operation<(Root<'_, Q>, A), driver::Buffer, driver::Shared>
    for Provided<O>
where
    A: buffer::raw::Arenas,
    O: Operation<storage::Scope<A>, Q>,
{
    type Output = O::Output;
    type Terminal = crate::Engine;
    type Proof = Closed;

    fn run<'d>(
        self,
        token: brand::Token<brand::Identity<'d>>,
        regions: (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
        shared: provided::Root<'d, driver::Buffer, driver::Shared, ()>,
        root: pin::Pin<&'d mut (Root<'_, Q>, A)>,
        backend: fixed::Loan<'d, Self::Terminal>,
    ) -> (Self::Output, fixed::End<'d, Self::Terminal, Self::Proof>)
    where
        Self: 'd,
    {
        let (root, arenas) = root.get_mut();
        let (state, source) = root.enter(shared, token, regions, backend);
        // SAFETY: the same generative borrow owns both the arenas and driver.
        // Output cannot retain 'd; reclamation precedes resident and backend teardown.
        let (roots, finish) = unsafe { arenas.enter() };
        let (output, end) = {
            let store = pin::pin!(roots);
            Closed::run::<storage::Scope<A>, _, _>(state, store.as_ref(), source, self.0)
        };
        finish();
        (output, end)
    }
}

pub(super) struct Build<S, O> {
    pub(super) plan: S,
    pub(super) operation: O,
}

impl<Q: ?Sized, S: storage::Build, O: Operation<S, Q>>
    fixed::Operation<Root<'_, Q>, driver::Buffer, driver::Shared> for Build<S, O>
{
    type Output = Result<O::Output, domain::Rejected<S::Error, O>>;
    type Terminal = Engine;
    type Proof = Closed;

    fn run<'d>(
        self,
        token: brand::Token<brand::Identity<'d>>,
        regions: (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
        shared: provided::Root<'d, driver::Buffer, driver::Shared, ()>,
        root: pin::Pin<&'d mut Root<'_, Q>>,
        backend: fixed::Loan<'d, Engine>,
    ) -> (Self::Output, fixed::End<'d, Self::Terminal, Self::Proof>)
    where
        Self: 'd,
    {
        let root = root.get_mut();
        let (mut state, source) = root.enter(shared, token, regions, backend);
        let prepared = self.plan.try_store();
        let mut backing = match prepared {
            Ok(backing) => backing,
            Err(error) => return Closed::reject(state, error, self.operation),
        };
        let built = self
            .plan
            .try_build(&mut backing, storage::Context::new(&mut state.local));
        match built {
            Ok(value) => {
                let store = pin::pin!(value);
                let (output, end) =
                    Closed::run::<S, _, _>(state, store.as_ref(), source, self.operation);
                (Ok(output), end)
            }
            Err(cause) => Closed::reject(state, cause, self.operation),
        }
    }
}

pub(super) struct Create<S, O> {
    pub(super) plan: S,
    pub(super) operation: O,
}

impl<Q: ?Sized, S: storage::Create, O: Operation<S, Q>>
    fixed::Operation<Root<'_, Q>, driver::Buffer, driver::Shared> for Create<S, O>
{
    type Output = Result<O::Output, domain::Rejected<S::Error, O>>;
    type Terminal = Engine;
    type Proof = Closed;

    fn run<'d>(
        self,
        token: brand::Token<brand::Identity<'d>>,
        regions: (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
        shared: provided::Root<'d, driver::Buffer, driver::Shared, ()>,
        root: pin::Pin<&'d mut Root<'_, Q>>,
        backend: fixed::Loan<'d, Engine>,
    ) -> (Self::Output, fixed::End<'d, Self::Terminal, Self::Proof>)
    where
        Self: 'd,
    {
        let (mut state, source) = root.get_mut().enter(shared, token, regions, backend);
        let built = self.plan.try_build(storage::Context::new(&mut state.local));
        match built {
            Ok(value) => {
                let store = pin::pin!(value);
                let (output, end) =
                    Closed::run::<S, _, _>(state, store.as_ref(), source, self.operation);
                (Ok(output), end)
            }
            Err(cause) => Closed::reject(state, cause, self.operation),
        }
    }
}

pub(super) struct Direct<S, O> {
    pub(super) plan: S,
    pub(super) operation: O,
}

impl<Q: ?Sized, S: storage::Direct, O: Operation<S, Q>>
    fixed::Operation<Root<'_, Q>, driver::Buffer, driver::Shared> for Direct<S, O>
{
    type Output = O::Output;
    type Terminal = Engine;
    type Proof = Closed;

    fn run<'d>(
        self,
        token: brand::Token<brand::Identity<'d>>,
        regions: (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
        shared: provided::Root<'d, driver::Buffer, driver::Shared, ()>,
        root: pin::Pin<&'d mut Root<'_, Q>>,
        backend: fixed::Loan<'d, Engine>,
    ) -> (Self::Output, fixed::End<'d, Self::Terminal, Self::Proof>)
    where
        Self: 'd,
    {
        let root = root.get_mut();
        let (state, source) = root.enter(shared, token, regions, backend);
        let value = storage::Direct::build(self.plan);
        let store = pin::pin!(value);
        Closed::run::<S, _, _>(state, store.as_ref(), source, self.operation)
    }
}

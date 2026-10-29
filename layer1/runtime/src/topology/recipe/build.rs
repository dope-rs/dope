use dope_core::{BranchError, driver::storage};

/// Builds one output from exact generative storage.
pub trait Build<S>: Sized
where
    S: storage::Plan,
{
    type Output<'life, 'p, 'd>: 'life
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd;
    type Error;

    fn build<'life, 'p, 'd>(
        self,
        storage: &'life S::Output<'p, 'd>,
    ) -> Result<Self::Output<'life, 'p, 'd>, Self::Error>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd;
}

impl<S, A, B> Build<S> for (A, B)
where
    S: storage::Plan,
    A: Build<S>,
    B: Build<S>,
{
    type Output<'life, 'p, 'd>
        = (A::Output<'life, 'p, 'd>, B::Output<'life, 'p, 'd>)
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd;
    type Error = BranchError<A::Error, B::Error>;

    fn build<'life, 'p, 'd>(
        self,
        storage: &'life S::Output<'p, 'd>,
    ) -> Result<Self::Output<'life, 'p, 'd>, Self::Error>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd,
    {
        let first = self.0.build(storage).map_err(BranchError::First)?;
        let second = self.1.build(storage).map_err(BranchError::Second)?;
        Ok((first, second))
    }
}

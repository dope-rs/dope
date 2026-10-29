use o3::base::cell::region;
use write::{context::modes, error};

use crate::network::listener::conn::write;

pub(in crate::network::listener::conn::write::context) trait Build<C, B> {
    type Receipt;
    type Error;

    fn build<H: AsMut<[u8; write::Prefix::CAPACITY]>>(
        self,
        state: &mut C,
        header: H,
    ) -> Result<write::Commit<H, B, Self::Receipt>, Self::Error>;
}

pub(in crate::network::listener::conn::write::context) struct Scoped<F>(
    pub(in crate::network::listener::conn::write::context) F,
);

impl<C, B, R, E, F> Build<C, B> for Scoped<F>
where
    F: for<'a, 'id> FnOnce(
        &mut C,
        write::Prefix<'a, 'id>,
    )
        -> Result<write::Commit<region::Proof<region::Identity<'id>>, B, R>, E>,
{
    type Receipt = R;
    type Error = E;

    fn build<H: AsMut<[u8; write::Prefix::CAPACITY]>>(
        self,
        state: &mut C,
        header: H,
    ) -> Result<write::Commit<H, B, R>, E> {
        write::Prefix::try_with(header, |prefix| self.0(state, prefix))
    }
}

pub(super) struct Buffer<F>(pub(super) F);

pub(super) enum Failure<B, R, E> {
    Build(E),
    Check(error::Mismatch<B, R>),
}

impl<C, B, R, E, F> Build<C, B> for Buffer<F>
where
    F: FnOnce(&mut C, &mut [u8]) -> Result<write::Draft<B, R>, E>,
{
    type Receipt = R;
    type Error = Failure<B, R, E>;

    fn build<H: AsMut<[u8; write::Prefix::CAPACITY]>>(
        self,
        state: &mut C,
        mut header: H,
    ) -> Result<write::Commit<H, B, R>, Self::Error> {
        let draft = self.0(state, header.as_mut()).map_err(Failure::Build)?;
        Ok(modes::Bounded::try_new(header, draft)
            .map_err(Failure::Check)?
            .validate())
    }
}

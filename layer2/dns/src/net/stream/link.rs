use dope::{
    core::driver::{route::Identity, schedule::ready},
    manifold::client::{close, conn, session::turns},
    net::link::egress,
};

use crate::{
    lookup::error,
    net::stream::cursor::Cursor,
    storage::{peer, queries, queries::index},
};

#[derive(Clone, Copy)]
pub(super) struct Bind<
    'life,
    's,
    'd: 'life,
    ID: Identity,
    const IDS: u32,
    const LANES: usize,
    const N: usize,
> {
    pub(super) token: conn::Id<'d, ID>,
    pub(super) peer: peer::Peer<'life, 's, 'd, LANES, N>,
    pub(super) wake: ready::Target<'d>,
    pub(super) ids: Cursor<IDS>,
    pub(super) blocked: bool,
}

#[derive(Clone, Copy)]
pub(super) struct Retiring<
    'life,
    's,
    'd: 'life,
    ID: Identity,
    const IDS: u32,
    const LANES: usize,
    const N: usize,
> {
    pub(super) bind: Bind<'life, 's, 'd, ID, IDS, LANES, N>,
    pub(super) branches: index::Retirement<LANES>,
    pub(super) error: Option<error::Kind>,
}

#[derive(Clone, Copy)]
pub(super) enum Link<
    'life,
    's,
    'd: 'life,
    ID: Identity,
    const IDS: u32,
    const LANES: usize,
    const N: usize,
> {
    Open(Bind<'life, 's, 'd, ID, IDS, LANES, N>),
    Retiring(Retiring<'life, 's, 'd, ID, IDS, LANES, N>),
}

impl<'life, 's, 'd: 'life, ID: Identity, const IDS: u32, const LANES: usize, const N: usize>
    Link<'life, 's, 'd, ID, IDS, LANES, N>
{
    pub(super) const fn token(self) -> conn::Id<'d, ID> {
        match self {
            Self::Open(bind) => bind.token,
            Self::Retiring(retiring) => retiring.bind.token,
        }
    }
}

impl<'life, 's, 'd: 'life, ID: Identity, const IDS: u32, const LANES: usize, const N: usize>
    Retiring<'life, 's, 'd, ID, IDS, LANES, N>
{
    pub(super) fn clear<'turn>(
        mut self,
        reason: close::Reason,
        ctx: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> (Option<Self>, egress::Cleared) {
        let mode = match self.error {
            Some(cause) => queries::Retirement::Fail(cause),
            None => queries::Retirement::from_close(reason),
        };
        let now = ctx.now();
        let work = ctx.work();
        match self
            .bind
            .peer
            .retire(ctx.state_token(), mode, now, work, &mut self.branches)
        {
            queries::RetireStream::Done => (None, egress::Cleared::Done),
            queries::RetireStream::Retry => (Some(self), egress::Cleared::Retry),
        }
    }
}

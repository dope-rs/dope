use o3::depot::queue::pool::fifo;

use crate::reply::{
    Item,
    arena::state::Signal,
    order, recv,
    retain::pool::{Acquire, Owner},
    slot::Push,
    usage::{self, Build, Footprint, Grouped, amount::Amount as _},
};

pub enum Target<'a, 'arena, 'credits, 'region, D: Footprint> {
    Single(Single<'a, 'arena, 'credits, 'region, D>),
    Collected(Collected<'a, 'arena, 'credits, 'region, D>),
    Discard,
}

pub struct Collected<'a, 'arena, 'credits, 'region, D: Footprint> {
    owner: &'a mut Owner<'arena, 'credits, 'region>,
    queue: &'a mut fifo::Fifo<'arena, Item<D>, false>,
}

pub struct Single<'a, 'arena, 'credits, 'region, D: Footprint> {
    destination: Collected<'a, 'arena, 'credits, 'region, D>,
    signal: &'a mut Signal<'region>,
    accepted: recv::Accepted,
}

impl<'a, 'arena, 'credits, 'region, D: Footprint> Target<'a, 'arena, 'credits, 'region, D> {
    pub(in crate::reply) fn new(
        owner: &'a mut Owner<'arena, 'credits, 'region>,
        queue: &'a mut fifo::Fifo<'arena, Item<D>, false>,
        signal: &'a mut Signal<'region>,
    ) -> Result<Self, order::Front> {
        let Some(accepted) = owner.acceptance else {
            return Ok(Self::Discard);
        };
        let destination = Collected { owner, queue };
        Ok(match accepted {
            recv::Accepted::Unit => return Err(order::Front::Denied),
            recv::Accepted::Complete => Self::Collected(destination),
            _ => Self::Single(Single {
                destination,
                signal,
                accepted,
            }),
        })
    }

    pub(in crate::reply) fn reserve(
        self,
        amount: D::Amount,
    ) -> Result<Option<Push<'a, 'arena, 'credits, 'region, D>>, Acquire> {
        match self {
            Self::Discard => Ok(None),
            Self::Single(single) => single.reserve(amount).map(Some),
            Self::Collected(collected) => collected
                .reserve(amount, recv::Acceptance::Collected, None)
                .map(Some),
        }
    }
}

impl<'a, 'arena, 'credits, 'region, D: Footprint> Single<'a, 'arena, 'credits, 'region, D> {
    fn reserve(self, amount: D::Amount) -> Result<Push<'a, 'arena, 'credits, 'region, D>, Acquire> {
        self.destination.reserve(
            amount,
            self.accepted.acceptance(),
            self.accepted.observes_data().then_some(self.signal),
        )
    }

    pub fn try_build<B: Build<D>>(
        self,
        builder: B,
    ) -> Result<(), usage::PushError<B, B::Error, D>> {
        build(
            builder,
            |amount| Ok(Some(self.reserve(amount)?)),
            Push::commit,
        )
    }
}

impl<'a, 'arena, 'credits, 'region, D: Footprint> Collected<'a, 'arena, 'credits, 'region, D> {
    fn reserve<S>(
        self,
        amount: D::Amount,
        policy: recv::Acceptance,
        signal: S,
    ) -> Result<Push<'a, 'arena, 'credits, 'region, D, S>, Acquire> {
        Push::reserve(self.queue, self.owner, amount, policy, signal)
    }

    pub fn try_build<B>(self, builder: B) -> Result<(), usage::PushError<B, B::Error, D::Group>>
    where
        D: Grouped,
        B: Build<D::Group>,
    {
        build(
            builder,
            |amount| {
                Ok(Some(self.reserve(
                    amount,
                    recv::Acceptance::Collected,
                    (),
                )?))
            },
            |held, group: D::Group| {
                held.insert(group.into(), |_| {});
            },
        )
    }
}

pub(in crate::reply) enum Reject {
    Front(order::Front),
    Acquire(Acquire),
}

impl From<Acquire> for Reject {
    fn from(cause: Acquire) -> Self {
        Self::Acquire(cause)
    }
}

pub(in crate::reply) fn build<V: Footprint, B: Build<V>, H>(
    builder: B,
    reserve: impl FnOnce(V::Amount) -> Result<Option<H>, Reject>,
    publish: impl FnOnce(H, V),
) -> Result<(), usage::PushError<B, B::Error, V>> {
    let declared = builder.usage();
    let held = match reserve(declared) {
        Ok(Some(held)) => held,
        Ok(None) => return Ok(()),
        Err(Reject::Acquire(Acquire::Backpressure(cause))) => {
            return Err(usage::PushError::Backpressure { builder, cause });
        }
        Err(Reject::Acquire(Acquire::Capacity(cause))) => {
            return Err(usage::PushError::Capacity { builder, cause });
        }
        Err(Reject::Front(front)) => return Err(usage::PushError::Front { builder, front }),
    };
    let item = builder
        .try_build()
        .map_err(|(builder, error)| usage::PushError::Build { builder, error })?;
    let actual = item.footprint();
    if actual != declared {
        return Err(usage::PushError::Footprint {
            value: item,
            declared: declared.usage(),
            actual: actual.usage(),
        });
    }
    publish(held, item);
    Ok(())
}

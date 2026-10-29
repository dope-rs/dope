use codec::{
    eof, staging,
    storage::{self, Edit},
};
use dope_core::driver::route::Identity;
use o3::buffer::{self, hold, resident};

use crate::client::{
    conn,
    session::{self, codec, response, turns},
};

type Owner<'slot, 'id, 'd, ID, N, P, M, S> = storage::Owner<
    'slot,
    'id,
    <M as Policy<'d, ID, N, P>>::Storage<S>,
    <M as Policy<'d, ID, N, P>>::State,
>;

/// Selects storage and delivery together for one exact codec class.
pub trait Policy<'d, ID: Identity, N: session::Session<'d, ID>, P: response::Plan> {
    type State;
    type Storage<S>;
    fn state(codec: &N::Codec, budget: resident::Budget<'d>) -> Self::State;
    fn storage<S>(codec: &N::Codec, source: S, state: &Self::State) -> Self::Storage<S>;
    fn source<S: buffer::raw::Source>(storage: &Self::Storage<S>) -> &S;
    fn edit<S>(storage: &mut Self::Storage<S>) -> Edit<'_, S>;
    fn restart<'a, S>(
        codec: &N::Codec,
        storage: &'a mut Self::Storage<S>,
        state: &Self::State,
    ) -> Edit<'a, S>;
    fn decode<S>(
        owner: &mut N,
        conn: conn::Id<'d, ID>,
        storage: &mut Self::Storage<S>,
        state: &mut Self::State,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        ctx: &mut turns::Context<'_, '_, 'd, N::ConnState, N::Send, ID>,
    ) -> codec::Step<P::Delivery>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume;
    fn finish<'slot, 'id, S>(
        owner: &mut N,
        input: Owner<'slot, 'id, 'd, ID, N, P, Self, S>,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        ctx: &mut turns::Context<'_, '_, 'd, N::ConnState, N::Send, ID>,
    ) -> codec::Finished<Owner<'slot, 'id, 'd, ID, N, P, Self, S>, P::Delivery>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume;
}

pub struct Plain;
pub struct Discard;
pub struct Stored;

trait End {
    type Codec: codec::Stateless;
    fn finish<'id, 'input, 'd, ID: Identity, S: buffer::Cursor + hold::raw::Retain<'d>>(
        owner: &impl session::Session<'d, ID, Codec = Self::Codec>,
        input: codec::Input<'id, 'input, 'd, S>,
    ) -> codec::End<'input, 'd, Self::Codec, S>
    where
        'd: 'input;
}

impl<C: eof::Finish> End for (Plain, C) {
    type Codec = C;
    fn finish<'id, 'input, 'd, ID: Identity, S: buffer::Cursor + hold::raw::Retain<'d>>(
        owner: &impl session::Session<'d, ID, Codec = C>,
        input: codec::Input<'id, 'input, 'd, S>,
    ) -> codec::End<'input, 'd, C, S>
    where
        'd: 'input,
    {
        eof::Finish::finish(owner.codec(), input)
    }
}

impl<C: codec::Stateless> End for (Discard, C) {
    type Codec = C;
    fn finish<'id, 'input, 'd, ID: Identity, S: buffer::Cursor + hold::raw::Retain<'d>>(
        _: &impl session::Session<'d, ID, Codec = C>,
        _: codec::Input<'id, 'input, 'd, S>,
    ) -> codec::End<'input, 'd, C, S>
    where
        'd: 'input,
    {
        Ok(codec::Finish::Empty)
    }
}

impl<'d, ID: Identity, N, P, M> Policy<'d, ID, N, P> for M
where
    P: response::Plan,
    N: session::Plain<'d, ID, Delivery = P::Delivery>,
    N::Codec: codec::Stateless,
    (M, N::Codec): End<Codec = N::Codec>,
{
    type State = ();
    type Storage<S> = S;
    fn state(_: &N::Codec, _: resident::Budget<'d>) {}
    fn storage<S>(_: &N::Codec, source: S, _: &()) -> S {
        source
    }
    fn source<S: buffer::raw::Source>(storage: &S) -> &S {
        storage
    }
    fn edit<S>(storage: &mut S) -> Edit<'_, S> {
        Edit::new(storage)
    }
    fn restart<'a, S>(_: &N::Codec, storage: &'a mut S, _: &()) -> Edit<'a, S> {
        Edit::new(storage)
    }
    fn decode<S>(
        owner: &mut N,
        _: conn::Id<'d, ID>,
        storage: &mut S,
        _: &mut (),
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        ctx: &mut turns::Context<'_, '_, 'd, N::ConnState, N::Send, ID>,
    ) -> codec::Step<P::Delivery>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        codec::Input::commit(
            storage,
            budget,
            limit,
            |input| match codec::Stateless::parse(owner.codec(), input) {
                Ok(codec::Parse::More(need)) => Ok(codec::Parse::More(need)),
                Ok(codec::Parse::Blocked) => Ok(codec::Parse::Blocked),
                Ok(codec::Parse::CapacityExhausted) => Ok(codec::Parse::CapacityExhausted),
                Ok(codec::Parse::Item { head, consumed }) => Ok(codec::Parse::Item {
                    head: session::Plain::deliver(owner, head, ctx),
                    consumed,
                }),
                Err(error) => Err(session::Plain::reject(owner, error, ctx)),
            },
        )
    }
    fn finish<'slot, 'id, S>(
        owner: &mut N,
        mut input: storage::Owner<'slot, 'id, S, ()>,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        ctx: &mut turns::Context<'_, '_, 'd, N::ConnState, N::Send, ID>,
    ) -> codec::Finished<storage::Owner<'slot, 'id, S, ()>, P::Delivery>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        let (storage, _) = input.parts();
        let result = codec::Input::with(storage, budget, limit, |input| {
            match <(Self, N::Codec) as End>::finish::<ID, _>(owner, input) {
                Ok(codec::Finish::Empty) => Ok(codec::Finish::Empty),
                Ok(codec::Finish::CapacityExhausted) => Ok(codec::Finish::CapacityExhausted),
                Ok(codec::Finish::Item(head)) => Ok(codec::Finish::Item(session::Plain::deliver(
                    owner, head, ctx,
                ))),
                Err(error) => Err(session::Plain::reject(owner, error, ctx)),
            }
        });
        (input, result)
    }
}

impl<'d, ID: Identity, N, P> Policy<'d, ID, N, P> for Stored
where
    P: response::Plan,
    N: session::Stored<'d, ID, Delivery = P::Delivery>,
    N::Codec: codec::Stateful,
{
    type State = <N::Codec as codec::Stateful>::State<'d>;
    type Storage<S> = <N::Codec as codec::Stateful>::Storage<S>;
    fn state(codec: &N::Codec, budget: resident::Budget<'d>) -> Self::State {
        codec::Stateful::state(codec, budget)
    }
    fn storage<S>(codec: &N::Codec, source: S, state: &Self::State) -> Self::Storage<S> {
        codec::Stateful::storage(codec, source, state)
    }
    fn source<S: buffer::raw::Source>(storage: &Self::Storage<S>) -> &S {
        <N::Codec as codec::Stateful>::source(storage)
    }
    fn edit<S>(storage: &mut Self::Storage<S>) -> Edit<'_, S> {
        <N::Codec as codec::Stateful>::edit(storage)
    }
    fn restart<'a, S>(
        codec: &N::Codec,
        storage: &'a mut Self::Storage<S>,
        state: &Self::State,
    ) -> Edit<'a, S> {
        codec::Stateful::restart(codec, storage, state)
    }
    fn decode<S>(
        owner: &mut N,
        conn: conn::Id<'d, ID>,
        storage: &mut Self::Storage<S>,
        state: &mut Self::State,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        ctx: &mut turns::Context<'_, '_, 'd, N::ConnState, N::Send, ID>,
    ) -> codec::Step<P::Delivery>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        let next = session::Stored::decode(owner, conn, storage, state, budget, limit, ctx);
        match next.0 {
            codec::Progress::Blocked => match session::Stored::blocked(owner, conn, state) {
                session::Blocking::Registered => codec::Step::blocked(),
                session::Blocking::Unsupported => codec::Step(codec::Progress::CapacityExhausted),
            },
            next => codec::Step(next),
        }
    }
    fn finish<'slot, 'id, S>(
        owner: &mut N,
        input: Owner<'slot, 'id, 'd, ID, N, P, Self, S>,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        ctx: &mut turns::Context<'_, '_, 'd, N::ConnState, N::Send, ID>,
    ) -> codec::Finished<Owner<'slot, 'id, 'd, ID, N, P, Self, S>, P::Delivery>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        session::Stored::finish(owner, input, budget, limit, ctx)
    }
}

mod ingress;
mod raw;
pub use ingress::Ingress;
pub(in crate::client::session) use ingress::Read;

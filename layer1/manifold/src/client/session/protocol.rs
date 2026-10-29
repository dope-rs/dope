//! Codec and ingress behavior for one service-bound endpoint.

use bindings::endpoint::State;
use dope_core::driver::route::Identity;
use dope_net::link::egress::data;
use o3::buffer::{self, hold, resident};

use crate::{
    client::{
        app::{self, connect, failure, request},
        close, conn,
        session::{self, codec, codec::staging, response},
    },
    service::bindings,
};

pub trait Spec<'life, 'd: 'life, P: ?Sized, ID: Identity, const MAX: usize>: Sized {
    type Codec: codec::Codec;
    type Mode;
    type Requests: request::Mode;
    type Response: response::Plan;
    type Connect: connect::Mode;
    type ConnState: Default;
    type Failure: failure::Mode;
    type Send: data::raw::Payload<'d>;

    fn codec<'a>(owner: &'a State<'life, 'd, P, Self, ID, MAX>) -> &'a Self::Codec;

    fn mode(
        _: &State<'life, 'd, P, Self, ID, MAX>,
        _: conn::Id<'d, ID>,
        _: &Self::ConnState,
    ) -> session::ReceiveMode {
        session::ReceiveMode::Framed
    }

    fn duplex<'id, 'e, 'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        _: &mut State<'life, 'd, P, Self, ID, MAX>,
        _: codec::Input<'id, 'input, 'd, R>,
        ctx: &mut session::Context<'_, 'e, 'life, 'd, P, Self, ID, MAX>,
    ) -> session::Duplex<'id>
    where
        'd: 'input,
    {
        ctx.request(close::Directive::reconnect(close::Reason::Protocol));
        session::Duplex::Close
    }

    fn finish<'e>(
        _: &mut State<'life, 'd, P, Self, ID, MAX>,
        _: close::Reason,
        _: &mut session::Context<'_, 'e, 'life, 'd, P, Self, ID, MAX>,
    ) -> session::Settlement {
        session::Settlement::Ready
    }

    fn eof<'e>(
        _: &mut State<'life, 'd, P, Self, ID, MAX>,
        _: &mut session::Context<'_, 'e, 'life, 'd, P, Self, ID, MAX>,
    ) -> app::Eof {
        app::Eof::Close
    }
}

/// Owns service-bound callbacks for a stateless codec.
pub trait Plain<'life, 'd: 'life, P: ?Sized, ID: Identity, const MAX: usize>:
    Spec<'life, 'd, P, ID, MAX>
where
    Self::Codec: codec::Stateless,
{
    fn deliver<'e, 'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        owner: &mut State<'life, 'd, P, Self, ID, MAX>,
        head: <Self::Codec as codec::Codec>::Head<'input, 'd, R>,
        ctx: &mut session::Context<'_, 'e, 'life, 'd, P, Self, ID, MAX>,
    ) -> <Self::Response as response::Plan>::Delivery
    where
        'd: 'input;

    fn reject<'e>(
        owner: &mut State<'life, 'd, P, Self, ID, MAX>,
        error: <Self::Codec as codec::Codec>::Error,
        ctx: &mut session::Context<'_, 'e, 'life, 'd, P, Self, ID, MAX>,
    ) -> <Self::Response as response::Plan>::Delivery;
}

/// Owns service-bound callbacks for a stateful codec.
pub trait Stored<'life, 'd: 'life, P: ?Sized, ID: Identity, const MAX: usize>:
    Spec<'life, 'd, P, ID, MAX>
where
    Self::Codec: codec::Stateful,
{
    fn blocked(
        _: &State<'life, 'd, P, Self, ID, MAX>,
        _: conn::Id<'d, ID>,
        _: &mut <Self::Codec as codec::Stateful>::State<'d>,
    ) -> session::Blocking;

    fn decode<S>(
        owner: &mut State<'life, 'd, P, Self, ID, MAX>,
        conn: conn::Id<'d, ID>,
        storage: &mut <Self::Codec as codec::Stateful>::Storage<S>,
        state: &mut <Self::Codec as codec::Stateful>::State<'d>,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        ctx: &mut session::Context<'_, '_, 'life, 'd, P, Self, ID, MAX>,
    ) -> codec::Step<<Self::Response as response::Plan>::Delivery>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume;

    fn finish<'slot, 'id, S>(
        owner: &mut State<'life, 'd, P, Self, ID, MAX>,
        input: codec::Owner<'slot, 'id, 'd, Self::Codec, S>,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        ctx: &mut session::Context<'_, '_, 'life, 'd, P, Self, ID, MAX>,
    ) -> codec::Finished<
        codec::Owner<'slot, 'id, 'd, Self::Codec, S>,
        <Self::Response as response::Plan>::Delivery,
    >
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume;
}

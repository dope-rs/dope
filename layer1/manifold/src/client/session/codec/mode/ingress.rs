use codec::storage;
use codec::{
    mode::{self, Policy},
    staging::{self, Buffer},
};
use dope_core::driver::route::Identity;
use dope_net::wire::{self, receive::Chunk};
use o3::buffer::resident;

use crate::{
    app::receive::Failure,
    client::session::{self, codec, response, turns},
};

type Stage<'d, N, ID> =
    <<<N as session::Session<'d, ID>>::Codec as codec::Codec>::Staging as staging::Seal>::Buf<'d>;
type Storage<'w, 'd, N, ID, W> = <<N as session::Parser<'d, ID>>::Mode as Policy<
    'd,
    ID,
    N,
    <N as session::Parser<'d, ID>>::Response,
>>::Storage<mode::raw::Source<'d, Stage<'d, N, ID>, Chunk<'w, 'd, W>>>;
type State<'d, N, ID> = <<N as session::Parser<'d, ID>>::Mode as Policy<
    'd,
    ID,
    N,
    <N as session::Parser<'d, ID>>::Response,
>>::State;

type Owner<'slot, 'id, 'w, 'd, ID, N, W> =
    storage::Owner<'slot, 'id, Storage<'w, 'd, N, ID, W>, State<'d, N, ID>>;

/// Owns one cumulative source, its scanner, and the separate protocol controls.
pub struct Ingress<'w, 'd: 'w, ID: Identity, N: session::Parser<'d, ID>, W: wire::Spec + 'd> {
    slot: storage::Slot<Storage<'w, 'd, N, ID, W>, State<'d, N, ID>>,
}

pub(in crate::client::session) enum Read<T> {
    Step(codec::Step<T>),
    Failed(Failure),
    Close,
}

impl<'w, 'd: 'w, ID: Identity, N: session::Parser<'d, ID>, W: wire::Spec + 'd>
    Ingress<'w, 'd, ID, N, W>
{
    pub(in crate::client::session) fn new(
        stage: Stage<'d, N, ID>,
        codec: &N::Codec,
        budget: resident::Budget<'d>,
    ) -> Self {
        let state = N::Mode::state(codec, budget);
        let source = mode::raw::Source::new(stage);
        let storage = N::Mode::storage(codec, source, &state);
        Self {
            slot: storage::Slot::new((storage, state)),
        }
    }

    pub(in crate::client::session) fn with<R>(
        &mut self,
        run: impl for<'slot, 'id> FnOnce(
            Owner<'slot, 'id, 'w, 'd, ID, N, W>,
        ) -> (Owner<'slot, 'id, 'w, 'd, ID, N, W>, R),
    ) -> R {
        self.slot.with(run)
    }

    pub(in crate::client::session) fn try_push(
        &mut self,
        chunk: Chunk<'w, 'd, W>,
    ) -> Result<(), Chunk<'w, 'd, W>> {
        let (storage, _) = self.slot.parts_mut();
        let received = &mut N::Mode::edit(storage).0.received;
        match received {
            mode::raw::Received::Vacant(_) => {
                *received = mode::raw::Received::Pending(chunk);
                Ok(())
            }
            mode::raw::Received::Staged { pending, .. } if pending.is_none() => {
                *pending = Some(chunk);
                Ok(())
            }
            mode::raw::Received::Staged { .. }
            | mode::raw::Received::Pending(_)
            | mode::raw::Received::Direct(_) => Err(chunk),
        }
    }

    pub(in crate::client::session) fn discard(&mut self, codec: &N::Codec) {
        let (storage, state) = self.slot.parts_mut();
        let source = if matches!(
            N::Mode::source(storage).received,
            mode::raw::Received::Direct(_)
        ) {
            N::Mode::restart(codec, storage, state)
        } else {
            N::Mode::edit(storage)
        };
        match &mut source.0.received {
            mode::raw::Received::Staged { pending, .. } => *pending = None,
            mode::raw::Received::Vacant(_) => {}
            received => *received = mode::raw::Received::Vacant(&[]),
        }
    }

    pub(in crate::client::session) fn clear(
        input: &mut Owner<'_, '_, 'w, 'd, ID, N, W>,
        codec: &N::Codec,
    ) {
        let (storage, state) = input.parts();
        let source = N::Mode::restart(codec, storage, state).0;
        source.received = mode::raw::Received::Vacant(&[]);
        if let Some(stage) = source.stage.buffered() {
            stage.clear();
        }
    }

    /// Activates only unconsumed owners; staged appends preserve the existing scanner.
    pub(in crate::client::session) fn prepare(
        &mut self,
        budget: &resident::Budget<'d>,
    ) -> Result<bool, Failure> {
        let (storage, _) = self.slot.parts_mut();
        N::Mode::edit(storage).0.prepare(budget)
    }

    pub(in crate::client::session) fn preserve(
        &mut self,
        target: Option<codec::Exact>,
        budget: &resident::Budget<'d>,
    ) -> Result<bool, Failure> {
        let (storage, _) = self.slot.parts_mut();
        N::Mode::edit(storage).0.preserve(target, budget)
    }

    pub(in crate::client::session) fn decode(
        &mut self,
        owner: &mut N,
        budget: &resident::Budget<'d>,
        ctx: &mut turns::Context<'_, '_, 'd, N::ConnState, N::Send, ID>,
    ) -> Read<<N::Response as response::Plan>::Delivery> {
        let (storage, state) = self.slot.parts_mut();
        let limit = N::Mode::source(storage).stage.limit();
        let (next, source) = match owner.mode(ctx.conn_id, ctx.state) {
            session::ReceiveMode::Framed => {
                let next = N::Mode::decode(owner, ctx.conn_id, storage, state, budget, limit, ctx);
                (Read::Step(next), N::Mode::edit(storage))
            }
            session::ReceiveMode::Paused => {
                (Read::Step(codec::Step::blocked()), N::Mode::edit(storage))
            }
            session::ReceiveMode::Duplex => {
                let source = N::Mode::edit(storage).0;
                let next = codec::Input::consume(source, budget, limit, |input| {
                    match owner.duplex(input, ctx) {
                        session::Duplex::Consumed(end) => (
                            Some(end),
                            Read::Step(codec::Step(codec::Progress::Item(
                                response::Outcome::committed(),
                            ))),
                        ),
                        session::Duplex::Blocked => (None, Read::Step(codec::Step::blocked())),
                        session::Duplex::Failed(error) => {
                            (None, Read::Failed(Failure::Input(error)))
                        }
                        session::Duplex::Close => (None, Read::Close),
                    }
                });
                (next, N::Mode::restart(owner.codec(), storage, state))
            }
        };
        source.0.direct();
        next
    }

    pub(in crate::client::session) fn finish<'slot, 'id>(
        mut input: Owner<'slot, 'id, 'w, 'd, ID, N, W>,
        owner: &mut N,
        budget: &resident::Budget<'d>,
        ctx: &mut turns::Context<'_, '_, 'd, N::ConnState, N::Send, ID>,
    ) -> codec::Finished<
        Owner<'slot, 'id, 'w, 'd, ID, N, W>,
        <N::Response as response::Plan>::Delivery,
    > {
        let limit = N::Mode::source(input.parts().0).stage.limit();
        N::Mode::finish(owner, input, budget, limit, ctx)
    }
}

impl<'w, 'd: 'w, ID: Identity, N, W> Ingress<'w, 'd, ID, N, W>
where
    N: session::Parser<'d, ID, Mode = mode::Stored>
        + session::Stored<'d, ID, Delivery = <N::Response as response::Plan>::Delivery>,
    N::Codec: codec::Stateful,
    W: wire::Spec + 'd,
{
    pub(in crate::client::session) fn state_mut(
        &mut self,
    ) -> &mut <N::Codec as codec::Stateful>::State<'d> {
        self.slot.parts_mut().1
    }
}

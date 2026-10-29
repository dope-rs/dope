use std::{cell, ops};

use dope::{
    core::driver::route::{Identity, KeyTag},
    net,
    net::{
        link::pool::{
            change,
            recv::{delivery, input, reserve::Choice},
        },
        wire,
        wire::{
            open,
            receive::{Chunk, Policy},
            terminal,
        },
    },
};
use o3::depot::queue::indexed::shared;

use crate::{
    core::{
        driver,
        driver::{schedule, schedule::app},
        io::{self, event::Kind},
    },
    manifold::{
        app::{creation, dispatch, dispatch::event, env, receive, receive::Failure},
        network::{listener, listener::conn},
        timing,
    },
    net::port::{active::Activation, table::Table},
    runtime::{executor, random},
    wait,
};

pub mod accept;
mod config;
mod queue;
mod requests;

pub use config::Config;
use queue::Queue;
mod plan;

pub use plan::Plan;

type Inner<'n, 'p, 's, 'scope, 'w, 'd, ID, T, W, F> = listener::Listener<
    'n,
    'p,
    's,
    'w,
    'd,
    ID,
    Queue<'scope, 'w, 'd, W, F, ID>,
    env::Bundle<T, W, timing::Balanced>,
>;

pub struct ListenerPort<'w, 'd: 'w, W: wire::Spec + 'w, ID: Identity = KeyTag<0>> {
    connections: Table<'d, Chunk<'w, 'd, W>, terminal::Error<'w, W>, conn::Id<'d, ID>>,
    accepts: shared::Fifo<conn::Id<'d, ID>>,
    waiters: wait::Queue<'d>,
    accept_error: cell::Cell<Option<listener::AcceptError>>,
}

impl<'w, 'd: 'w, W: wire::Spec + 'w, ID: Identity> ListenerPort<'w, 'd, W, ID> {
    fn retain_error(&self, error: Option<listener::AcceptError>) {
        if let Some(error) = error {
            self.accept_error.set(Some(error));
        }
    }

    fn progress(&self) -> schedule::Progress<'d> {
        if self.accept_error.get().is_some() && self.waiters.len() != wait::Length::ZERO {
            return schedule::Progress::Runnable;
        }
        self.connections.progress()
    }

    fn activate(&self, id: conn::Id<'d, ID>, work: app::Work<'_, 'd>) -> receive::Outcome {
        let slot = match self.accepts.try_entry(id.index().into_usize()) {
            Ok(slot) => slot,
            Err(cause) => return receive::Outcome::Failed(Failure::Accept(cause)),
        };
        if self.waiters.wake_one(work) == wait::WakeStatus::Pending {
            return receive::Outcome::Failed(Failure::Contract);
        }
        if self.connections.defer(id) == Activation::Rejected {
            return receive::Outcome::Failed(Failure::Contract);
        }
        slot.push::<0>(id);
        receive::Outcome::Continue
    }

    pub fn handle(&self) -> Handle<'_, 'w, 'd, W, ID> {
        Handle { port: self }
    }
}

#[pin_project::pin_project(!Unpin)]
pub struct Listener<
    'n,
    'p,
    's,
    'scope,
    'w,
    'd: 's + 'w,
    ID: Identity,
    T: net::Transport,
    W: input::Execution + 'd,
    F: FnMut(open::Error<'w, W>),
> {
    #[pin]
    inner: Inner<'n, 'p, 's, 'scope, 'w, 'd, ID, T, W, F>,
    port: &'scope ListenerPort<'w, 'd, W, ID>,
}

impl<
    'n,
    'p,
    's,
    'scope,
    'w,
    'd: 'w,
    ID: Identity,
    T: net::Transport,
    W: input::Execution + 'd,
    F: FnMut(open::Error<'w, W>),
> executor::raw::Finishes<'d, Self> for Listener<'n, 'p, 's, 'scope, 'w, 'd, ID, T, W, F>
where
    <W::Receive as Policy<W>>::Reserve: Choice<W, W::Retained>,
    <W::Receive as Policy<W>>::Credit: delivery::Ownership<W>,
{
    fn call<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (mut owner, install, proof) = finish.into_parts();
        let child = executor::raw::Finish::new(owner.as_mut().project().inner, install, proof);
        let child = <Inner<'n, 'p, 's, 'scope, 'w, 'd, ID, T, W, F> as executor::raw::Finishes<
            'd,
            _,
        >>::call(child);
        let (install, proof) = child.into_finish();
        executor::raw::Finish::new(owner, install, proof)
    }
}

impl<
    'n,
    'p,
    's,
    'scope,
    'w,
    'd: 'w,
    ID: Identity,
    T: net::Transport,
    W: input::Execution + 'd,
    F: FnMut(open::Error<'w, W>),
> event::Turn<'d> for Listener<'n, 'p, 's, 'scope, 'w, 'd, ID, T, W, F>
where
    Inner<'n, 'p, 's, 'scope, 'w, 'd, ID, T, W, F>:
        event::Turn<'d, Access = dispatch::raw::Retained>,
    <W::Receive as Policy<W>>::Reserve: Choice<W, W::Retained>,
    <W::Receive as Policy<W>>::Credit: delivery::Ownership<W>,
{
    type Access = dispatch::raw::Retained;

    /// Retains accept/tuning completions until their waiter wake can consume one credit.
    /// Resolution before that callback consumes no application credit.
    fn dispatch<'app, 'driver, 'turn, 'root>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                schedule::Turn<'turn, 'd>,
                dispatch::raw::Context<'driver, 'root, 'd, Self::Access>,
            ),
        >,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'root,
    {
        let (mut owner, (), (done, turn, driver)) = call.into_parts();
        let kind = done.into_kind();
        if matches!(&kind, Kind::Accept(_) | Kind::Tuning(_)) && turn.app().remaining() == 0 {
            return ops::ControlFlow::Break(kind.into());
        }
        let result = event::Turn::dispatch(executor::raw::Call::new(
            owner.as_mut().project().inner,
            (),
            (kind.into(), turn, driver),
        ));
        let this = owner.as_ref().get_ref();
        this.port.retain_error(this.inner.accept_error());
        result
    }
}

impl<
    'n,
    'p,
    's,
    'scope,
    'w,
    'd: 'scope + 'w,
    ID: Identity,
    T: net::Transport,
    W: input::Execution + 'd,
    F: FnMut(open::Error<'w, W>),
> Listener<'n, 'p, 's, 'scope, 'w, 'd, ID, T, W, F>
where
    <W::Receive as Policy<W>>::Credit: delivery::Ownership<W>,
    <W::Receive as Policy<W>>::Reserve: Choice<W, W::Retained>,
{
    pub fn adopt<C, O, R>(
        port: &'scope ListenerPort<'w, 'd, W, ID>,
        config: Config<T, R>,
        failed: F,
        hash: random::HashState<'d>,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'net, 'gate, 'store> FnOnce(
            Listener<'net, 'gate, 'store, 'scope, 'w, 'd, ID, T, W, F>,
            &mut C,
        ) -> O,
    ) -> Result<O, creation::Error>
    where
        T: net::Listen,
        R: change::Prepare<'w, 'd, ID, W>,
    {
        config
            .prepare::<W, ID>(port.connections.capacity())
            .with_driver(
                Queue::<W, F, ID> { port, failed },
                hash,
                owner,
                context,
                |inner, owner| {
                    port.accept_error.set(None);
                    build(Listener { inner, port }, owner)
                },
            )
    }
}

pub struct Handle<'scope, 'w, 'd: 'w, W: wire::Spec, ID: Identity = KeyTag<0>> {
    port: &'scope ListenerPort<'w, 'd, W, ID>,
}

impl<W: wire::Spec, ID: Identity> Copy for Handle<'_, '_, '_, W, ID> {}

impl<W: wire::Spec, ID: Identity> Clone for Handle<'_, '_, '_, W, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'scope, 'w, 'd: 'w, W: wire::Spec, ID: Identity> Handle<'scope, 'w, 'd, W, ID> {
    pub fn accept(self) -> accept::Accept<'scope, 'w, 'd, W, ID> {
        accept::Accept::new(self)
    }
}

mod raw;

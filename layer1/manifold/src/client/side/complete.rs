use std::pin;

use dope_core::{
    driver::{
        retained,
        route::{Identity, table},
        schedule,
    },
    io::event::net::{connect, send, socket},
};

use crate::{
    app::{env, receive::ingress::Dispatch},
    client::{
        app, attempt,
        conn::engine::{
            self, liveness,
            ops::{close::ClosePhase, connect::Connect},
            transfer::{flush::Flush, send::Policy, wakes},
        },
        side::{self, mode::Mode, registry, turn::Turn},
    },
};

pub(in crate::client) trait Complete<'w, 'd: 'w, ID: Identity + 'd, A, S, E>:
    Mode<'d, ID> + Sized
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID> + app::Scheduling<'d, ID>,
    A::Input: app::Policy<'w, 'd, ID, A> + Dispatch<A::Wire>,
    A::Requests: Policy<'w, 'd, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    fn flush<'p>(
        machine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, Self>>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) where
        A::Continuation: Flush<'p, 'w, 'd, ID, A, S, E, Self>,
        Self: liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, Self>: ClosePhase<'d, ID, A, S, E>,
    {
        <A::Continuation as Flush<'p, 'w, 'd, ID, A, S, E, Self>>::flush(machine, turn, driver);
    }

    fn send<'p>(
        machine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, Self>>,
        done: send::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<(), send::Completion<'d>>
    where
        engine::Machine<'p, 'w, 'd, ID, A, S, E, Self>: wakes::Completion<'d>,
    {
        wakes::Completion::complete(machine, done, turn, driver);
        Ok(())
    }

    fn socket<'p>(
        machine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, Self>>,
        done: socket::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) where
        engine::Machine<'p, 'w, 'd, ID, A, S, E, Self>: Connect<'d, ID, A, S, E>,
    {
        Connect::socket(machine, done, turn, driver);
    }

    fn connect<'p>(
        mut machine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, Self>>,
        done: connect::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<(), connect::Completion<'d>>
    where
        engine::Machine<'p, 'w, 'd, ID, A, S, E, Self>: Connect<'d, ID, A, S, E>,
    {
        match Self::try_side(machine.as_mut(), done, driver) {
            Ok(()) => Ok(()),
            Err(done) => Connect::try_connect(machine, done, turn, driver),
        }
    }

    fn try_side<'p>(
        _: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, Self>>,
        done: connect::Completion<'d>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<(), connect::Completion<'d>> {
        Err(done)
    }
}

impl<'w, 'd: 'w, ID: Identity + 'd, A, S, E> Complete<'w, 'd, ID, A, S, E> for side::Disabled
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID> + app::Scheduling<'d, ID>,
    A::Input: app::Policy<'w, 'd, ID, A> + Dispatch<A::Wire>,
    A::Requests: Policy<'w, 'd, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
}

impl<'w, 'd: 'w, ID: Identity + 'd, A, S, E, I> Complete<'w, 'd, ID, A, S, E>
    for side::Enabled<&registry::Registry<'d, I, ID>>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID> + app::Scheduling<'d, ID>,
    A::Input: app::Policy<'w, 'd, ID, A> + Dispatch<A::Wire>,
    A::Requests: Policy<'w, 'd, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    I: registry::Intent<'d, ID>,
{
    fn flush<'p>(
        mut machine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, Self>>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) where
        A::Continuation: Flush<'p, 'w, 'd, ID, A, S, E, Self>,
        Self: liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, Self>: ClosePhase<'d, ID, A, S, E>,
    {
        let target = machine.as_ref().backoff();
        let this = machine.as_mut().project();
        this.links.liveness_mut().resume_retry(target);
        this.side.registry.retry();
        <A::Continuation as Flush<'p, 'w, 'd, ID, A, S, E, Self>>::flush(machine, turn, driver);
    }

    fn send<'p>(
        mut machine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, Self>>,
        done: send::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<(), send::Completion<'d>>
    where
        engine::Machine<'p, 'w, 'd, ID, A, S, E, Self>: wakes::Completion<'d>,
    {
        let primary = table::Capacity::from(machine.as_ref().project_ref().links.capacity());
        if !primary.contains(done.token().slot()) {
            Turn::new(machine).send(done, driver)
        } else {
            wakes::Completion::complete(machine.as_mut(), done, turn, driver);
            Ok(())
        }
    }

    fn socket<'p>(
        mut machine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, Self>>,
        done: socket::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) where
        engine::Machine<'p, 'w, 'd, ID, A, S, E, Self>: Connect<'d, ID, A, S, E>,
    {
        let primary = table::Capacity::from(machine.as_ref().project_ref().links.capacity());
        if !primary.contains(done.token().slot()) {
            Turn::new(machine.as_mut()).socket(done, driver);
        } else {
            Connect::socket(machine.as_mut(), done, turn.reborrow(), driver);
        }
    }

    fn try_side<'p>(
        machine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, Self>>,
        done: connect::Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Result<(), connect::Completion<'d>> {
        let primary = table::Capacity::from(machine.as_ref().project_ref().links.capacity());
        if !primary.contains(done.token().slot()) {
            Turn::new(machine).connect(done, driver)
        } else {
            Err(done)
        }
    }
}

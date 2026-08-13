use std::{convert, ops, pin};

use dope_core::{
    driver::{
        retained, route,
        schedule::{self, ready},
    },
    io::event::receiving,
};
use dope_net::{
    self,
    link::{
        event,
        pool::{self, input},
    },
    wire,
};

use crate::receive;

mod admission;

pub(crate) const IOV_CAP: usize = 32;

type Storage<'a, 'd, const ID: u8, P> = &'a mut pool::Connections<
    'd,
    ID,
    <P as Policy<'d, ID>>::Transport,
    <P as Policy<'d, ID>>::Wire,
    <P as Policy<'d, ID>>::State,
    <P as Policy<'d, ID>>::Input,
    <P as Policy<'d, ID>>::Payload,
    IOV_CAP,
>;

pub trait Dispatch: receive::Delivery + Sized {
    fn initial<'a, 'c, 'owner, 'd, W: wire::Wire>(
        completion: &receiving::Completion<'d>,
        turn: schedule::Turn<'a, 'd>,
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
    ) -> Option<admission::Admission<'a, 'c, 'owner, 'd, W>>
    where
        'd: 'owner;

    fn resumed<'a, 'c, 'owner, 'd, W: wire::Wire>(
        first: bool,
        pending: bool,
        ready: ready::Key<'d>,
        turn: schedule::Turn<'a, 'd>,
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
    ) -> Option<admission::Admission<'a, 'c, 'owner, 'd, W>>
    where
        'd: 'owner;

    fn handle_data<'a, 'c, 'owner, 'd, const ID: u8, P>(
        policy: pin::Pin<&mut P>,
        data: receiving::DataCompletion<'d>,
        work: admission::Admission<'a, 'c, 'owner, 'd, P::Wire>,
    ) -> Option<(
        pool::Key<'d, ID>,
        &'a mut retained::Context<'c, 'owner, 'd>,
        schedule::Turn<'a, 'd>,
    )>
    where
        'd: 'owner,
        P: Policy<'d, ID, Input = Self>;
}

#[derive(Clone, Copy)]
pub enum Empty {
    /// The wire emitted no application chunk and did not report a discard.
    NoChunk,
    /// The wire consumed input without emitting an application chunk.
    Discarded,
}

#[derive(Clone, Copy)]
pub enum Finish {
    /// No application chunk was delivered; the wire distinction is preserved.
    Empty(Empty),
    /// The application accepted a delivered chunk or batch.
    Chunk,
    /// The application requested closure after pending egress drains.
    CloseAfter,
}

#[derive(Clone, Copy)]
pub enum CloseCause {
    /// The local driver cancelled the receive.
    Local,
    /// Transport input failed, starved, or ended during a data transition.
    Transport,
    /// Local bounded storage could not retain more application input.
    Capacity,
    /// Receive bookkeeping or application processing exceeded its contract.
    Protocol,
    /// The peer reached EOF.
    Remote,
}

/// Shared receive state-machine policy. Transport accounting lives here;
/// endpoint lifecycle remains with each implementor.
pub trait Policy<'d, const ID: u8>: Sized {
    type Transport: dope_net::Transport;
    type Wire: wire::Wire;
    type State;
    type Payload;
    type Input: Dispatch + input::Mode<Self::Wire>;

    fn storage<'a>(self: pin::Pin<&'a mut Self>) -> Storage<'a, 'd, ID, Self>;

    fn receive<'input>(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        input: <Self::Input as receive::Delivery>::Value<'input, 'd, Self::Wire>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome;

    fn finish(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        finish: Finish,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn close(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        cause: CloseCause,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn dispatch(
        mut self: pin::Pin<&mut Self>,
        completion: receiving::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> ops::ControlFlow<receiving::Completion<'d>> {
        let Some(work) = Self::Input::initial::<Self::Wire>(&completion, turn.reborrow(), driver)
        else {
            return ops::ControlFlow::Break(completion);
        };
        handle(self.as_mut(), completion, work);
        Self::storage(self)
            .ingress()
            .flush(turn.maintenance(), driver);
        ops::ControlFlow::Continue(())
    }

    fn resume(
        mut self: pin::Pin<&mut Self>,
        target: route::Token,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let ready = {
            let storage = Self::storage(self.as_mut());
            let Some((_, slot)) = storage.by_target(target) else {
                return;
            };
            slot.io().ready_key()
        };
        let resumed = Self::storage(self.as_mut()).ingress().resume(target);
        let pending = Self::storage(self.as_mut()).ingress().has_resumed(target);
        if !resumed && !pending {
            return;
        }

        let mut first = true;
        loop {
            let pending = Self::storage(self.as_mut()).ingress().has_resumed(target);
            let Some(work) =
                Self::Input::resumed::<Self::Wire>(first, pending, ready, turn.reborrow(), driver)
            else {
                break;
            };
            let completion = Self::storage(self.as_mut()).ingress().pop_resumed(target);
            let Some(completion) = completion else {
                break;
            };
            first = false;
            handle(self.as_mut(), completion, work);
        }
        Self::storage(self)
            .ingress()
            .flush(turn.maintenance(), driver);
    }
}

fn handle<'a, 'c, 'owner, 'd, const ID: u8, P>(
    mut policy: pin::Pin<&mut P>,
    completion: receiving::Completion<'d>,
    work: admission::Admission<'a, 'c, 'owner, 'd, P::Wire>,
) where
    P: Policy<'d, ID>,
{
    match completion.classify() {
        receiving::Classification::Data(data) => {
            if let Some((key, driver, turn)) = P::Input::handle_data(policy.as_mut(), data, work) {
                P::close(policy, key, CloseCause::Protocol, turn, driver);
            }
        }
        receiving::Classification::Control(control) => {
            let close_cause = match control.event() {
                receiving::Control::Eof => CloseCause::Remote,
                receiving::Control::Failed(_)
                | receiving::Control::BufferExhausted
                | receiving::Control::Starved => CloseCause::Transport,
                receiving::Control::Cancelled => CloseCause::Local,
            };
            let dispatch = P::storage(policy.as_mut())
                .ingress()
                .dispatch_control::<convert::Infallible>(control);
            let dispatch = match dispatch {
                event::ControlDispatch::Ready(dispatch) => dispatch,
                event::ControlDispatch::Parked(event::ParkRecv::Close(key)) => {
                    let (driver, turn) = work.commit();
                    P::close(policy.as_mut(), key, CloseCause::Protocol, turn, driver);
                    return;
                }
                event::ControlDispatch::Parked(event::ParkRecv::Deferred) => {
                    work.commit();
                    return;
                }
            };
            let (driver, turn) = work.commit();
            apply(
                policy,
                dispatch,
                close_cause,
                turn,
                driver,
                |_, _, never, _, _| match never {},
            );
        }
    }
}

impl Dispatch for receive::Borrowed {
    fn initial<'a, 'c, 'owner, 'd, W: wire::Wire>(
        completion: &receiving::Completion<'d>,
        turn: schedule::Turn<'a, 'd>,
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
    ) -> Option<admission::Admission<'a, 'c, 'owner, 'd, W>>
    where
        'd: 'owner,
    {
        if matches!(completion.event(), crate::RecvEvent::Data(_)) {
            admission::Admission::reserve(turn, driver, 0).ok()
        } else {
            Some(admission::Admission::open(turn, driver))
        }
    }

    fn resumed<'a, 'c, 'owner, 'd, W: wire::Wire>(
        first: bool,
        _: bool,
        ready: ready::Key<'d>,
        turn: schedule::Turn<'a, 'd>,
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
    ) -> Option<admission::Admission<'a, 'c, 'owner, 'd, W>>
    where
        'd: 'owner,
    {
        let preceding = usize::from(!first);
        let work = match admission::Admission::reserve(turn, driver, preceding) {
            Ok(work) => work,
            Err(driver) => {
                driver.driver_ref().ready().activate_ready(ready);
                return None;
            }
        };
        Some(work)
    }

    fn handle_data<'a, 'c, 'owner, 'd, const ID: u8, P>(
        mut policy: pin::Pin<&mut P>,
        data: receiving::DataCompletion<'d>,
        work: admission::Admission<'a, 'c, 'owner, 'd, P::Wire>,
    ) -> Option<(
        pool::Key<'d, ID>,
        &'a mut retained::Context<'c, 'owner, 'd>,
        schedule::Turn<'a, 'd>,
    )>
    where
        'd: 'owner,
        P: Policy<'d, ID, Input = Self>,
    {
        let (prepared, mut data) = match P::storage(policy.as_mut()).ingress().reserve_data(data) {
            event::DataReservation::Ready {
                prepared,
                completion,
            } => (prepared, completion),
            event::DataReservation::Parked(event::ParkRecv::Close(key)) => {
                let (driver, turn) = work.commit();
                return Some((key, driver, turn));
            }
            event::DataReservation::Parked(event::ParkRecv::Deferred)
            | event::DataReservation::Drop => {
                work.commit();
                return None;
            }
        };
        let dispatch = prepared.dispatch(&mut data, work.capacity());
        let (driver, turn) = work.commit_batch(&dispatch);
        apply(
            policy,
            dispatch,
            CloseCause::Transport,
            turn,
            driver,
            |policy, key, input, turn, driver| P::receive(policy, key, input, turn, driver),
        );
        None
    }
}

impl Dispatch for receive::Retained {
    fn initial<'a, 'c, 'owner, 'd, W: wire::Wire>(
        _: &receiving::Completion<'d>,
        turn: schedule::Turn<'a, 'd>,
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
    ) -> Option<admission::Admission<'a, 'c, 'owner, 'd, W>>
    where
        'd: 'owner,
    {
        Some(admission::Admission::open(turn, driver))
    }

    fn resumed<'a, 'c, 'owner, 'd, W: wire::Wire>(
        first: bool,
        pending: bool,
        ready: ready::Key<'d>,
        turn: schedule::Turn<'a, 'd>,
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
    ) -> Option<admission::Admission<'a, 'c, 'owner, 'd, W>>
    where
        'd: 'owner,
    {
        if !first && !turn.application().take() {
            if pending {
                driver.driver_ref().ready().activate_ready(ready);
            }
            return None;
        }
        Some(admission::Admission::open(turn, driver))
    }

    fn handle_data<'a, 'c, 'owner, 'd, const ID: u8, P>(
        mut policy: pin::Pin<&mut P>,
        data: receiving::DataCompletion<'d>,
        work: admission::Admission<'a, 'c, 'owner, 'd, P::Wire>,
    ) -> Option<(
        pool::Key<'d, ID>,
        &'a mut retained::Context<'c, 'owner, 'd>,
        schedule::Turn<'a, 'd>,
    )>
    where
        'd: 'owner,
        P: Policy<'d, ID, Input = Self>,
    {
        let (prepared, data) = match P::storage(policy.as_mut()).ingress().reserve_data(data) {
            event::DataReservation::Ready {
                prepared,
                completion,
            } => (prepared, completion),
            event::DataReservation::Parked(event::ParkRecv::Close(key)) => {
                let (driver, turn) = work.commit();
                return Some((key, driver, turn));
            }
            event::DataReservation::Parked(event::ParkRecv::Deferred)
            | event::DataReservation::Drop => {
                work.commit();
                return None;
            }
        };
        let dispatch = prepared.dispatch_retained(data);
        let (driver, turn) = work.commit();
        apply(
            policy,
            dispatch,
            CloseCause::Transport,
            turn,
            driver,
            |policy, key, input, turn, driver| P::receive(policy, key, input, turn, driver),
        );
        None
    }
}

fn apply<'d, const ID: u8, P, C, F>(
    mut policy: pin::Pin<&mut P>,
    dispatch: event::DispatchRecv<'d, ID, C>,
    close_cause: CloseCause,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
    receive: F,
) where
    P: Policy<'d, ID>,
    F: FnOnce(
        pin::Pin<&mut P>,
        pool::Key<'d, ID>,
        C,
        schedule::Turn<'_, 'd>,
        &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome,
{
    use dope_net::link::event::DispatchRecv;

    match dispatch {
        DispatchRecv::Drop => {}
        DispatchRecv::Close(key) => P::close(policy, key, close_cause, turn.reborrow(), driver),
        DispatchRecv::Overrun(key) => {
            if let Some(slot) = P::storage(policy.as_mut()).get_mut(key) {
                slot.abort();
            }
            P::close(policy, key, CloseCause::Protocol, turn.reborrow(), driver);
        }
        DispatchRecv::NoChunk(key) => P::finish(
            policy,
            key,
            Finish::Empty(Empty::NoChunk),
            turn.reborrow(),
            driver,
        ),
        DispatchRecv::Discarded(key) => P::finish(
            policy,
            key,
            Finish::Empty(Empty::Discarded),
            turn.reborrow(),
            driver,
        ),
        DispatchRecv::Chunk(key, chunk) => {
            match receive(policy.as_mut(), key, chunk, turn.reborrow(), driver) {
                crate::Outcome::Ok => {
                    P::finish(policy, key, Finish::Chunk, turn.reborrow(), driver);
                }
                crate::Outcome::Overrun => {
                    if let Some(slot) = P::storage(policy.as_mut()).get_mut(key) {
                        slot.abort();
                    }
                    P::close(policy, key, CloseCause::Protocol, turn.reborrow(), driver);
                }
                crate::Outcome::Capacity => {
                    if let Some(slot) = P::storage(policy.as_mut()).get_mut(key) {
                        slot.abort();
                    }
                    P::close(policy, key, CloseCause::Capacity, turn.reborrow(), driver);
                }
                crate::Outcome::CloseAfter => {
                    P::storage(policy.as_mut()).ingress().set_close_after(key);
                    P::finish(policy, key, Finish::CloseAfter, turn.reborrow(), driver);
                }
            }
        }
    }
}

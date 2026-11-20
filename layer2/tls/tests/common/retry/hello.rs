use std::{collections::VecDeque, convert::Infallible, ops::ControlFlow};

use dope::{
    core::{
        driver::life,
        io::event::{self, net::recv},
    },
    net::link::{
        egress::data::raw::Payload,
        event::{ControlRecv, Dispatch, DispatchRecv, ParkRecv, Reservation},
        slot::transmit::{Finalize, outcome::Retention},
    },
};

use o3::buffer::write::consume::Consume as _;

use crate::common::retry::{Error, Held, Key, Mode, Pool, UPDATES, native::next, pending, settle, submit};

/// Retains real receive completions until the handshake send completion is held,
/// then processes authenticated input before settling that original send owner.
pub(super) fn establish<'p, 'w, 'd: 'w, B: Payload<'d>>(
    pool: &mut Pool<'p, 'w, 'd, B>,
    keys: [Key<'d>; 2],
    scope: &mut life::Scope<'_, 'd>,
    mode: Mode,
) -> Result<Option<Held>, Error> {
    let hold = matches!(mode, Mode::Held | Mode::Updates);
    let expected = [
        if matches!(mode, Mode::Updates) {
            UPDATES
        } else {
            1
        },
        1,
    ];
    {
        let mut driver = scope.context();
        for key in keys.into_iter().take(if hold { 1 } else { 2 }) {
            pool.recv()
                .arm(key, &mut driver)
                .map_err(|failure| Error::Submit(failure.cause))?;
        }
    }
    let mut received = [0; 2];
    let mut queued = VecDeque::new();
    let mut held = None;
    let mut observed: Option<Held> = None;
    'events: loop {
        let event = if held.is_some() || observed.is_some() {
            match queued.pop_front() {
                Some(done) => event::Kind::Recv(done),
                None => next(scope)?.into_kind(),
            }
        } else {
            next(scope)?.into_kind()
        };
        match event {
            event::Kind::Recv(done) => 'receive: {
                if hold
                    && held.is_none()
                    && observed.is_none()
                    && pool
                        .inspect()
                        .get(keys[0])
                        .is_some_and(|conn| conn.send_status().is_inflight())
                {
                    queued.push_back(done);
                    continue 'events;
                }
                let done =
                    match done.dispatch(|_, _, value| ControlFlow::<_, Infallible>::Break(value)) {
                        ControlFlow::Break(done) => done,
                        ControlFlow::Continue(never) => match never {},
                    };
                let data = match done.classify() {
                    recv::Kind::Data(bytes) => match pool.recv().data(
                        bytes, |_, _| (), |plan| plan.map(|data| data.scoped()),
                    ).1 {
                        Reservation::Ready(data) => data,
                        Reservation::Control(control) => DispatchRecv::Control(control),
                        Reservation::Parked(ParkRecv::Deferred) => break 'receive,
                        Reservation::Parked(_) => return Err(Error::Reserve("parked")),
                        Reservation::Stale(_) => return Err(Error::Reserve("stale")),
                        Reservation::Drop => return Err(Error::Reserve("drop")),
                    },
                    recv::Kind::Control(control) => match pool.recv().dispatch(control).1 {
                        Dispatch::Settled => break 'receive,
                        Dispatch::Ready(control) => DispatchRecv::Control(control),
                        Dispatch::Parked(ParkRecv::Deferred) => break 'receive,
                        Dispatch::Parked(_) => return Err(Error::RecvEvent("parked")),
                        Dispatch::Retired(_) => return Err(Error::RecvEvent("retired")),
                        Dispatch::Observed { .. } => return Err(Error::RecvEvent("observed")),
                        Dispatch::Stale { .. } => return Err(Error::RecvEvent("stale")),
                        Dispatch::Rejected { .. } => return Err(Error::RecvEvent("rejected")),
                    },
                };
                let key = match data {
                    DispatchRecv::Chunk(key, mut chunk) => {
                        let index = keys
                            .iter()
                            .position(|other| *other == key)
                            .ok_or(Error::Key)?;
                        let bytes = chunk.as_ref();
                        let valid = bytes.iter().all(|byte| *byte == b'A' + index as u8);
                        let count = bytes.len();
                        received[index] += count;
                        chunk.try_consume(count).map_err(Error::Consume)?;
                        if !valid || received[index] > expected[index] {
                            return Err(Error::Record);
                        }
                        key
                    }
                    DispatchRecv::Control(ControlRecv::NoChunk(key)) => key,
                    DispatchRecv::Control(ControlRecv::Failed { error, .. }) => {
                        return Err(Error::Receive(error));
                    }
                    DispatchRecv::Control(ControlRecv::Credit { cause, .. }) => {
                        return Err(Error::Credit(cause));
                    }
                    _ => return Err(Error::Control),
                };
                submit(pool, key, scope)?;
            }
            event::Kind::Send(done) => {
                if hold && observed.is_none() {
                    if held.is_some() {
                        return Err(Error::Flight);
                    }
                    held = Some(done);
                } else {
                    let bytes = done.outcome();
                    let key = settle(pool, done, scope)?;
                    if let (true, Some(held), Ok(bytes)) =
                        (key == keys[0], observed.as_mut(), bytes)
                    {
                        held.sent += bytes.into_raw();
                    }
                }
            }
            _ => return Err(Error::Handshake),
        }
        pending(pool, scope, |_| {})?;
        if matches!(mode, Mode::Failed) {
            let sender = pool.egress_mut(keys[0]).ok_or(Error::Sender)?;
            match sender
                .connection
                .sending()
                .finish(sender.flights, &mut scope.context())
            {
                Finalize::Settled | Finalize::Armed(()) => {}
                Finalize::Rejected(cause) => return Err(Error::Rejected(cause)),
                Finalize::Abandoned => return Err(Error::Settlement),
            }
        }
        {
            let (_, mut driver, mut control) = scope.split();
            let turn = control.begin();
            pool.recv()
                .flush(turn.turn().maint(), &mut driver)
                .map_err(|failure| Error::Submit(failure.cause))?;
        }
        if hold && observed.is_none() && received[0] == expected[0] {
            let conn = pool.inspect().get(keys[0]).ok_or(Error::Key)?;
            let status = conn.send_status();
            observed = Some(Held {
                inflight: status.is_inflight(),
                retained: matches!(status.retention(), Retention::Held),
                rearm: pool.inspect().has_rearm(),
                sent: 0,
            });
            settle(pool, held.take().ok_or(Error::Flight)?, scope)?;
            let mut driver = scope.context();
            pool.recv()
                .arm(keys[1], &mut driver)
                .map_err(|failure| Error::Submit(failure.cause))?;
        }
        if received == expected
            && (!matches!(mode, Mode::Failed)
                || pool
                    .inspect()
                    .get(keys[0])
                    .is_some_and(|conn| conn.terminal.is_pending() && !conn.terminal.has_send()))
            && keys.into_iter().all(|key| {
                pool.get_mut(key)
                    .is_some_and(|conn| !conn.send_status().is_inflight())
            })
        {
            return Ok(observed);
        }
    }
}

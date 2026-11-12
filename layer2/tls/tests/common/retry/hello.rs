use std::{collections::VecDeque, convert::Infallible, ops::ControlFlow};

use dope::{
    core::{
        driver::life,
        io::event::{self, net::recv},
    },
    net::link::{
        event::{ControlRecv, Dispatch, DispatchRecv, ParkRecv, Reservation},
        slot::transmit::{Finalize, outcome::Retention},
    },
};

use crate::common::{
    Read,
    retry::{Error, Held, Key, Mode, Pool, native::next, pending, settle, submit},
};

/// Retains real receive completions until the handshake send completion is held,
/// then processes authenticated input before settling that original send owner.
pub(super) fn establish<'p, 'w, 'd: 'w>(
    pool: &mut Pool<'p, 'w, 'd>,
    keys: [Key<'d>; 2],
    scope: &mut life::Scope<'_, 'd>,
    mode: Mode,
) -> Result<Option<Held>, Error> {
    {
        let mut driver = scope.context();
        for key in keys
            .into_iter()
            .take(if matches!(mode, Mode::Held) { 1 } else { 2 })
        {
            pool.recv()
                .arm(key, &mut driver)
                .map_err(|failure| Error::Submit(failure.cause))?;
        }
    }
    let mut ready = [false; 2];
    let mut queued = VecDeque::new();
    let mut held = None;
    let mut observed = None;
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
                if matches!(mode, Mode::Held)
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
                    recv::Kind::Data(bytes) => match pool.recv().data(bytes, |_, _| ()) {
                        Reservation::Ready(data) => data.scoped(),
                        Reservation::Control(control) => DispatchRecv::Control(control),
                        Reservation::Parked(ParkRecv::Deferred) => break 'receive,
                        Reservation::Parked(_) => return Err(Error::Reserve("parked")),
                        Reservation::Stale(_) => return Err(Error::Reserve("stale")),
                        Reservation::Drop => return Err(Error::Reserve("drop")),
                    },
                    recv::Kind::Control(control) => match pool.recv().dispatch(control) {
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
                        let mut marker = Vec::new();
                        chunk
                            .drive(
                                Ok(()),
                                &mut Read(|bytes: &[u8]| {
                                    marker.extend_from_slice(bytes);
                                    bytes.len()
                                }),
                            )
                            .map_err(Error::Consume)?;
                        if marker.as_slice() != [b'A' + index as u8] {
                            return Err(Error::Record);
                        }
                        ready[index] = true;
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
                if matches!(mode, Mode::Held) && observed.is_none() {
                    if held.is_some() {
                        return Err(Error::Flight);
                    }
                    held = Some(done);
                } else {
                    settle(pool, done, scope)?;
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
        if matches!(mode, Mode::Held) && observed.is_none() && ready[0] {
            let conn = pool.inspect().get(keys[0]).ok_or(Error::Key)?;
            let status = conn.send_status();
            observed = Some(Held {
                inflight: status.is_inflight(),
                retained: matches!(status.retention(), Retention::Held),
                rearm: pool.inspect().has_rearm(),
            });
            settle(pool, held.take().ok_or(Error::Flight)?, scope)?;
            let mut driver = scope.context();
            pool.recv()
                .arm(keys[1], &mut driver)
                .map_err(|failure| Error::Submit(failure.cause))?;
        }
        if ready == [true; 2]
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

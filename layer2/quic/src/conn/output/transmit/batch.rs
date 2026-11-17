use o3::buffer::{self, write};
use std::time;

use crate::conn;
use crate::conn::life::datagram::Datagrams;
use crate::conn::output::recovery;
use crate::conn::output::transmit::{self, builder, builder::pending, permit, stage};
use crate::conn::stream::state::receive;
use crate::conn::wire::{packet, path, permits};
use crate::stream;
use crate::wire::{frame, packet::kind, stream::Initiator};
use permits::Reservation as _;

use app::{datagram::direct, early, rtt, terminal};
use builder::app;
use permit::fit;
use receive::control;
use recovery::epochs;

enum Emission {
    Committed,
    Absent,
}

pub(super) struct Outcome {
    pub(super) destination: Option<transmit::Destination>,
    pub(super) effects: transmit::Effects,
}

pub(super) struct Batch<'a, const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment> {
    connection: &'a mut conn::Machine<DOMAIN, B, D>,
    destination: transmit::Policy,
    records: builder::Records,
}

impl<'a, const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment> Batch<'a, DOMAIN, B, D> {
    pub(super) fn new(conn: &'a mut conn::Machine<DOMAIN, B, D>, policy: transmit::Policy) -> Self {
        Self {
            connection: conn,
            destination: policy,
            records: builder::Records::new(),
        }
    }

    pub(super) fn fill<S: packet::Sink>(
        self,
        sink: &mut S,
        now: time::Instant,
        packets: usize,
        max_len: usize,
    ) -> Result<(), transmit::Error> {
        self.try_fill(sink, now, packets, max_len, &mut |prepared, now| {
            prepared.commit(now);
        })
    }

    pub(super) fn report<const OBSERVE: bool, S: packet::Sink>(
        self,
        sink: &mut S,
        now: time::Instant,
        packets: usize,
        max_len: usize,
    ) -> (Outcome, Result<(), transmit::Error>) {
        let mut outcome = Outcome {
            destination: None,
            effects: transmit::Effects::EMPTY,
        };
        let result = self.try_fill(sink, now, packets, max_len, &mut |prepared, now| {
            let effects = if OBSERVE {
                transmit::Effects::packet(&prepared)
            } else {
                transmit::Effects::EMPTY
            };
            let target = prepared.destination();
            prepared.commit(now);
            outcome.destination = Some(target);
            outcome.effects.insert(effects);
        });
        (outcome, result)
    }

    fn try_fill<S: packet::Sink, C>(
        mut self,
        sink: &mut S,
        now: time::Instant,
        packets: usize,
        max_len: usize,
        commit: &mut C,
    ) -> Result<(), transmit::Error>
    where
        C: for<'pending> FnMut(pending::Packet<'pending, DOMAIN, B, D>, time::Instant),
    {
        let pto = match stage::Stage::new(self.connection)
            .try_one_rtt(now)
            .map_err(|error| match error {
                transmit::Admission::Timing(error) => transmit::Error::Timing(error),
                error => transmit::Error::Admission(error.into_error()),
            })? {
            stage::Outcome::Ready(pto) => pto,
            stage::Outcome::Closed => return Ok(()),
        };
        if self
            .connection
            .try_terminal(now)
            .map_err(transmit::Error::Timing)?
        {
            if packets != 0 && self.connection.egress.lifecycle.close().is_some() {
                let mtu = max_len.min(self.connection.egress.cc.metrics().current_mtu() as usize);
                if let Some(ceiling) = permit::Permit::new(self.connection).send_limit(mtu) {
                    self.emit_packet(sink, now, ceiling, commit, |builder, dst, ceiling| {
                        terminal::close(builder, dst, ceiling)
                    })?;
                }
            }
            return Ok(());
        }
        let packets = self
            .connection
            .handshake
            .keys
            .write()
            .map_or(packets, |key| key.limit(packets));
        self.connection
            .egress
            .derived
            .reconcile(&mut self.connection.path, &mut self.connection.control)
            .map_err(transmit::Admission::from)?;
        let work = packets
            .min(conn::MAX_BATCH_PACKETS)
            .saturating_mul(conn::PACKET_CONTROL_CAPACITY);
        self.connection
            .control
            .queue()
            .reconcile(work)
            .map_err(transmit::Admission::from)?;
        let can_control = permit::Permit::new(self.connection).can_stream_control();
        let work = if can_control { work } else { 0 };
        control::Drain::new(
            &mut self.connection.streams.state,
            &mut self.connection.control,
            work,
        )
        .drain()
        .map_err(transmit::Admission::from)?;
        let last = self.connection.egress.activity.last_activity;
        let mut sent = false;
        let commit = &mut |prepared: pending::Packet<'_, DOMAIN, B, D>, now| {
            commit(prepared, now);
            sent = true;
        };
        let mtu = max_len.min(self.connection.egress.cc.metrics().current_mtu() as usize);
        let mut left = packets;
        let mut hs_done = false;

        let result: Result<bool, transmit::Admission> = (|| {
            stage::Stage::new(self.connection).snapshot(work, can_control)?;

            if left != 0 && self.connection.egress.recovery.probe.allowance.is_armed() {
                let keys = &self.connection.handshake.keys;
                let count = match self.connection.egress.recovery.probe.epoch {
                    Some(conn::Epoch::Initial) => {
                        keys.long.initial().map_or(left, |key| key.limit(left))
                    }
                    Some(conn::Epoch::Handshake) => {
                        keys.long.handshake().map_or(left, |key| key.limit(left))
                    }
                    Some(conn::Epoch::Application) if !keys.has_write() => {
                        keys.long.early().map_or(left, |key| key.limit(left))
                    }
                    _ => left,
                };
                let floor = left - count;
                while left != floor
                    && self.connection.egress.recovery.probe.allowance.is_armed()
                    && self
                        .connection
                        .egress
                        .recovery
                        .probe
                        .epoch
                        .is_some_and(|epoch| {
                            fit::can_probe(&permit::Permit::new(self.connection), epoch)
                        })
                {
                    let Some(ceiling) = permit::Permit::new(self.connection).send_limit(mtu) else {
                        break;
                    };
                    let Emission::Committed =
                        self.emit_packet(sink, now, ceiling, commit, |builder, dst, ceiling| {
                            builder.build_probe(now, dst, ceiling)
                        })?
                    else {
                        break;
                    };
                    left -= 1;
                }
                if left != 0 && left == floor {
                    return Ok(true);
                }
            }

            if let Some(key) = self.connection.handshake.keys.long.initial() {
                let floor = left - key.limit(left);
                while left != floor {
                    let Some(intent) =
                        permit::Permit::new(self.connection).plan_crypto::<kind::Initial>(now)
                    else {
                        break;
                    };
                    let Some(ceiling) = permit::Permit::new(self.connection).send_limit(mtu) else {
                        break;
                    };
                    let Emission::Committed = self.emit_packet(
                        sink,
                        now,
                        ceiling,
                        commit,
                        move |builder, dst, ceiling| match intent {
                            permit::CryptoIntent::AckOnly => builder
                                .build_crypto::<kind::Initial, false, true>(now, dst, ceiling),
                            permit::CryptoIntent::CongestionControlledPacket => {
                                builder
                                    .build_crypto::<kind::Initial, false, false>(now, dst, ceiling)
                            }
                        },
                    )?
                    else {
                        break;
                    };
                    left -= 1;
                    self.connection.egress.lifecycle.sent_initial = true;
                }
                if left != 0 && left == floor {
                    return Ok(true);
                }
            }

            if left != 0
                && !self.connection.handshake.keys.has_write()
                && let Some(key) = self.connection.handshake.keys.long.early()
            {
                let floor = left - key.limit(left);
                while left != floor && permit::Permit::new(self.connection).can_send(now) {
                    let Some(ceiling) = permit::Permit::new(self.connection).send_limit(mtu) else {
                        break;
                    };
                    let Emission::Committed =
                        self.emit_packet(sink, now, ceiling, commit, |builder, dst, ceiling| {
                            early::build(builder, dst, ceiling, false)
                        })?
                    else {
                        break;
                    };
                    left -= 1;
                }
                if left != 0 && left == floor {
                    return Ok(true);
                }
            }

            if let Some(key) = self.connection.handshake.keys.long.handshake() {
                let floor = left - key.limit(left);
                while left != floor {
                    let Some(intent) =
                        permit::Permit::new(self.connection).plan_crypto::<kind::Handshake>(now)
                    else {
                        break;
                    };
                    let Some(ceiling) = permit::Permit::new(self.connection).send_limit(mtu) else {
                        break;
                    };
                    let Emission::Committed = self.emit_packet(
                        sink,
                        now,
                        ceiling,
                        commit,
                        move |builder, dst, ceiling| match intent {
                            permit::CryptoIntent::AckOnly => builder
                                .build_crypto::<kind::Handshake, false, true>(now, dst, ceiling),
                            permit::CryptoIntent::CongestionControlledPacket => builder
                                .build_crypto::<kind::Handshake, false, false>(
                                now, dst, ceiling,
                            ),
                        },
                    )?
                    else {
                        break;
                    };
                    left -= 1;
                }
                if left != 0 && left == floor {
                    return Ok(true);
                }
            }

            if self.connection.handshake.keys.has_write() {
                for _ in 0..4096u32 {
                    if left == 0 {
                        break;
                    }
                    let Some(intent) = permit::Permit::new(self.connection).plan_one_rtt(now)
                    else {
                        break;
                    };
                    if intent == permit::AppIntent::CongestionControlledDatagram {
                        break;
                    }
                    let Some(ceiling) = permit::Permit::new(self.connection).send_limit(mtu) else {
                        break;
                    };
                    let (done, flight) = match intent {
                        permit::AppIntent::AckOnly => {
                            let Emission::Committed = self.emit_ack(sink, now, mtu, commit)? else {
                                break;
                            };
                            (false, false)
                        }
                        permit::AppIntent::CongestionControlledPacket => {
                            let Some(pn) = conn::Epoch::Application
                                .bind_mut(&mut self.connection.egress.recovery.spaces)
                                .next()
                            else {
                                break;
                            };
                            let permit = self.connection.path.output().permit(
                                &self.connection.control,
                                ceiling,
                                &pn,
                            );
                            let Some(outcome) = self.emit_with(
                                sink,
                                now,
                                permit.route(),
                                permit.ceiling(),
                                commit,
                                |builder, dst, ceiling| {
                                    rtt::build::<false, false, DOMAIN, B, D>(
                                        builder,
                                        now,
                                        dst,
                                        ceiling,
                                        Some(permit),
                                        S::PAD_TO_CEILING,
                                    )
                                },
                                |commit| {
                                    let done = commit.controls().iter().any(|control| {
                                        control.kind() == frame::Kind::HandshakeDone
                                    });
                                    (done, commit.plan().flags.is_in_flight())
                                },
                            )?
                            else {
                                break;
                            };
                            outcome
                        }
                        permit::AppIntent::CongestionControlledDatagram => {
                            break;
                        }
                    };
                    left -= 1;
                    if done {
                        hs_done = true;
                    }
                    if intent == permit::AppIntent::CongestionControlledPacket && !flight {
                        break;
                    }
                }
                let payload = Datagrams::<DOMAIN, B, false, D>::new(self.connection).max_payload();
                while left != 0 {
                    let Some(data) = self.connection.egress.datagrams.pending_datagrams.front()
                    else {
                        break;
                    };
                    // A path change may invalidate an already owned unreliable payload.
                    if payload.is_none_or(|max| data.len() > max) {
                        self.connection.egress.datagrams.pop_front();
                        left -= 1;
                        continue;
                    }
                    if !permit::Permit::new(self.connection).can_send(now) {
                        break;
                    }
                    let Some(ceiling) = permit::Permit::new(self.connection).send_limit(mtu) else {
                        break;
                    };
                    let Emission::Committed =
                        self.emit_packet(sink, now, ceiling, commit, |builder, dst, limit| {
                            direct::build(builder, now, dst, limit)
                        })?
                    else {
                        break;
                    };
                    left -= 1;
                }
                if left != 0
                    && self
                        .connection
                        .receive
                        .packet_numbers
                        .is_ack_ready::<kind::OneRtt>(now)
                    && matches!(self.emit_ack(sink, now, mtu, commit)?, Emission::Committed)
                {
                    left -= 1;
                }
                if left != 0
                    && let Some(probe) = permit::Permit::new(self.connection).next_probe()
                    && permit::Permit::new(self.connection).can_send(now)
                    && let Some(ceiling) = permit::Permit::new(self.connection).send_limit(max_len)
                {
                    self.emit_packet(sink, now, ceiling, commit, |builder, dst, limit| {
                        terminal::probe(builder, now, dst, probe, limit)
                    })?;
                }
            }
            Ok(false)
        })();

        if hs_done && matches!(self.connection.peer.local, Initiator::Server) {
            epochs::Epochs::new(self.connection).discard(conn::Epoch::Handshake);
        }

        let timing = if sent {
            let changed = last != self.connection.egress.activity.last_activity;
            self.connection.try_timers(now, changed, pto)
        } else {
            Ok(())
        };
        match (result, timing) {
            (Ok(exhausted), Ok(())) => {
                if exhausted {
                    stage::Stage::new(self.connection).exhaust_long();
                }
                Ok(())
            }
            (Err(cause), Ok(())) => Err(transmit::Error::Admission(cause)),
            (Ok(_), Err(timing)) => Err(transmit::Error::Timing(timing)),
            (Err(cause), Err(timing)) => Err(transmit::Error::Finalize {
                admission: cause,
                timing,
            }),
        }
    }

    fn emit_packet<'emission, S: packet::Sink, C>(
        &'emission mut self,
        sink: &mut S,
        now: time::Instant,
        limit: usize,
        commit: &mut C,
        build: impl FnOnce(
            builder::Builder<'emission, DOMAIN, B, D>,
            &mut write::Append<'_>,
            usize,
        ) -> transmit::Built<'emission, DOMAIN, B, D>,
    ) -> Result<Emission, transmit::Admission>
    where
        C: for<'pending> FnMut(pending::Packet<'pending, DOMAIN, B, D>, time::Instant),
    {
        match self.emit_with(
            sink,
            now,
            path::OutputRoute::ACTIVE,
            limit,
            commit,
            build,
            |_| (),
        )? {
            Some(()) => Ok(Emission::Committed),
            None => Ok(Emission::Absent),
        }
    }

    fn emit_with<'emission, S: packet::Sink, T, F, C>(
        &'emission mut self,
        sink: &mut S,
        now: time::Instant,
        route: path::OutputRoute,
        limit: usize,
        commit: &mut C,
        build: impl FnOnce(
            builder::Builder<'emission, DOMAIN, B, D>,
            &mut write::Append<'_>,
            usize,
        ) -> transmit::Built<'emission, DOMAIN, B, D>,
        inspect: F,
    ) -> Result<Option<T>, transmit::Admission>
    where
        F: FnOnce(&pending::Packet<'_, DOMAIN, B, D>) -> T,
        C: for<'pending> FnMut(pending::Packet<'pending, DOMAIN, B, D>, time::Instant),
    {
        let path = self.connection.path.output();
        let Some(destination) = path
            .target(route)
            .and_then(|routed| self.destination.resolve(routed))
        else {
            return Ok(None);
        };
        let limit = if route.path().is_none() {
            limit.min(usize::from(path.max_payload().get()))
        } else {
            limit
        }
        .min(destination.max_payload());
        let Some(mut permit) = sink.reserve(limit, &destination)? else {
            return Ok(None);
        };
        let limit = permit.limit();
        let built = {
            let mut out = permit.out();
            build(
                builder::Builder::new(self.connection, destination, &mut self.records),
                &mut out,
                limit,
            )
        };
        let prepared = match built {
            Ok(Some(prepared)) => prepared,
            Ok(None) => {
                permit.rollback();
                return Ok(None);
            }
            Err(error) => {
                permit.rollback();
                return Err(error);
            }
        };
        let ready = match permit.validate(prepared.destination().overhead()) {
            Ok(ready) => ready,
            Err(permit) => {
                permit.rollback();
                return Ok(None);
            }
        };
        let value = inspect(&prepared);
        commit(prepared, now);
        <S::Reservation<'_> as permits::Reservation>::publish(ready);
        Ok(Some(value))
    }

    fn emit_ack<S: packet::Sink, C>(
        &mut self,
        sink: &mut S,
        now: time::Instant,
        bytes: usize,
        commit: &mut C,
    ) -> Result<Emission, transmit::Admission>
    where
        C: for<'pending> FnMut(pending::Packet<'pending, DOMAIN, B, D>, time::Instant),
    {
        let Some(limit) = permit::Permit::new(self.connection).send_limit(bytes) else {
            return Ok(Emission::Absent);
        };
        self.emit_packet(sink, now, limit, commit, |builder, dst, limit| {
            rtt::build::<false, true, DOMAIN, B, D>(
                builder,
                now,
                dst,
                limit,
                None,
                S::PAD_TO_CEILING,
            )
        })
    }
}

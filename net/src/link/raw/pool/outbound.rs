use std::error::Error;
use std::fmt;
use std::io;

use dope_core::backend::{RawSqe, RetainedSqe, Sqe, StableSqeSource};
use dope_core::driver::DriverContext;
use dope_core::driver::submission::Submission;
use dope_core::driver::token::kind::{CONNECT, CREATE};
use dope_core::driver::token::{SlotIndex, Token};
use dope_core::io::socket::addr::Addr;
use dope_core::io::{ConnectEvent, SocketEvent};

use super::super::core::{Core, Outbound};
use super::super::event::{ConnectStep, SocketStep};
use super::Pool;
use crate::Transport;
use crate::link::slot::Slot;
use crate::wire::{OpenReservation, Wire};

#[derive(Debug)]
/// A permanent failure before a socket becomes an outbound pool slot.
pub enum OpenFailure<E> {
    Socket(io::Error),
    Wire(E),
}

impl<E: fmt::Display> fmt::Display for OpenFailure<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Socket(error) => write!(formatter, "socket preparation failed: {error}"),
            Self::Wire(error) => write!(formatter, "wire open failed: {error}"),
        }
    }
}

impl<E: Error + 'static> Error for OpenFailure<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Socket(error) => Some(error),
            Self::Wire(error) => Some(error),
        }
    }
}

struct ConnectSubmission(RawSqe);

// SAFETY: this private source is created only after Establish installs the
// address and Pool retains both it and the fixed fd until completion.
unsafe impl StableSqeSource for ConnectSubmission {
    fn into_raw(self) -> RawSqe {
        self.0
    }
}

pub struct OutboundSlot<'a, 'd, W: Wire, S> {
    pub slot: &'a mut Slot<'d, W, S>,
    pub token: Token,
}

pub trait OutboundPool<'d> {
    type Transport: Transport;
    type Wire: Wire;
    type State: Outbound;

    fn send_slot(
        &mut self,
        idx: SlotIndex,
    ) -> Option<OutboundSlot<'_, 'd, Self::Wire, Self::State>>;

    fn for_each_outbound_target(&mut self, visit: impl FnMut(Token));

    fn submit_socket_with_state(
        &mut self,
        socket_params: (i32, i32, i32),
        make_state: impl FnOnce(SlotIndex, &mut o3::cell::RegionToken<'d>) -> Self::State,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Result<Option<SlotIndex>, OpenFailure<<Self::Wire as Wire>::OpenError>>;

    fn drive_socket_cqe<X>(
        &mut self,
        ud: Token,
        event: &SocketEvent,
        driver: &mut DriverContext<'_, 'd>,
        prepare: impl FnOnce(
            &Slot<'d, Self::Wire, Self::State>,
        ) -> (
            X,
            Option<(Addr, <Self::Transport as Transport>::StreamConfig)>,
        ),
    ) -> SocketStep<X>;

    fn drive_connect_cqe<X>(
        &mut self,
        ud: Token,
        event: &ConnectEvent,
        driver: &mut DriverContext<'_, 'd>,
        peek: impl FnOnce(&Slot<'d, Self::Wire, Self::State>) -> X,
    ) -> ConnectStep<X>;
}

impl<'d, const ID: u8, T, W, S> OutboundPool<'d> for Pool<'d, ID, T, W, S>
where
    T: Transport,
    W: Wire,
    S: Outbound,
{
    type Transport = T;
    type Wire = W;
    type State = S;

    fn send_slot(
        &mut self,
        idx: SlotIndex,
    ) -> Option<OutboundSlot<'_, 'd, Self::Wire, Self::State>> {
        let (slot, key) = self.slab.get_index_mut(idx.raw())?;
        let token = Token::from_key(key);
        if slot.core.is_closing() || slot.core.is_send_inflight() {
            return None;
        }
        Some(OutboundSlot { slot, token })
    }

    fn for_each_outbound_target(&mut self, mut visit: impl FnMut(Token)) {
        for slot in self.slab.values_mut() {
            let token = slot.token();
            let establish = slot.state.establish();
            if establish.is_connecting() {
                visit(token.with_kind(CONNECT));
            } else if !establish.is_done() {
                visit(Token::framework(slot.core.fd.token_index()).with_kind(CREATE));
            }
        }
    }

    fn submit_socket_with_state(
        &mut self,
        socket_params: (i32, i32, i32),
        make_state: impl FnOnce(SlotIndex, &mut o3::cell::RegionToken<'d>) -> S,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Result<Option<SlotIndex>, OpenFailure<W::OpenError>> {
        let Some(reservation) = self.slab.vacant_entry() else {
            return Ok(None);
        };
        let Some(token) = self.rearm.bind(reservation.token()) else {
            return Ok(None);
        };
        let idx = token.token().slot();
        let Some(outbound_slot) = self.reservation.slot(idx) else {
            return Ok(None);
        };
        let fd = outbound_slot.bind(driver.driver_ref());
        let (domain, socket_type, protocol) = socket_params;
        let ud = token.token();
        let sqe = match Sqe::socket(domain, socket_type, protocol, &fd, ud) {
            Ok(sqe) => sqe,
            Err(error) => {
                drop(driver.guard(fd));
                return Err(OpenFailure::Socket(error));
            }
        };
        let open = match W::prepare_open(&mut self.runtime) {
            Ok(Some(open)) => open,
            Ok(None) => {
                drop(driver.guard(fd));
                return Ok(None);
            }
            Err(error) => {
                drop(driver.guard(fd));
                return Err(OpenFailure::Wire(error));
            }
        };
        if driver.push(sqe).is_err() {
            drop(driver.guard(fd));
            return Ok(None);
        }
        let (wire, send) = open.commit();
        let state = make_state(idx, driver.region_token());
        let slot = Slot::<W, S>::new(Core::new(fd, T::KERNEL_DISCARD), wire, send, token, state);
        reservation.insert(slot);
        self.refresh_wake(idx);
        Ok(Some(idx))
    }

    fn drive_socket_cqe<X>(
        &mut self,
        ud: Token,
        event: &SocketEvent,
        driver: &mut DriverContext<'_, 'd>,
        prepare: impl FnOnce(&Slot<'d, W, S>) -> (X, Option<(Addr, T::StreamConfig)>),
    ) -> SocketStep<X> {
        let Some(parts) = ud.parts() else {
            return SocketStep::Failed { peeked: None };
        };
        let (peeked, submitted) = {
            let Some(slot) = self.slab.get_parts_mut(parts) else {
                return SocketStep::Failed { peeked: None };
            };
            let (peeked, prepared) = prepare(&*slot);
            let submitted =
                if let (SocketEvent::Created, Some((sock_addr, config))) = (event, prepared) {
                    if T::submit_stream_tuning(driver, config, &slot.core.fd) {
                        let (ptr, len) = slot.state.establish().begin(sock_addr);
                        let submitted = driver
                            .push_retained(RetainedSqe::from_stable(ConnectSubmission(
                                RawSqe::connect(&slot.core.fd, ptr, len, ud),
                            )))
                            .is_ok();
                        if !submitted {
                            slot.state.establish().abort();
                        }
                        submitted
                    } else {
                        false
                    }
                } else {
                    false
                };
            (peeked, submitted)
        };
        if submitted {
            SocketStep::Connecting
        } else {
            if let Some(slot) = self.slab.remove_parts(parts) {
                slot.close(driver);
            }
            SocketStep::Failed {
                peeked: Some(peeked),
            }
        }
    }

    fn drive_connect_cqe<X>(
        &mut self,
        ud: Token,
        event: &ConnectEvent,
        driver: &mut DriverContext<'_, 'd>,
        peek: impl FnOnce(&Slot<'d, W, S>) -> X,
    ) -> ConnectStep<X> {
        let Some(parts) = ud.parts() else {
            return ConnectStep::Drop { peeked: None };
        };
        let idx = parts.slot();
        let failed = matches!(event, ConnectEvent::Failed(_));
        let (peeked, armed, rearm) = {
            let Some(slot) = self.slab.get_parts_mut(parts) else {
                return ConnectStep::Drop { peeked: None };
            };
            if !slot.state.establish().is_connecting() {
                let peeked = (!slot.state.establish().is_done()).then(|| peek(&*slot));
                return ConnectStep::Drop { peeked };
            }
            let peeked = peek(&*slot);
            if failed {
                slot.state.establish().abort();
                (peeked, false, slot.rearm_token())
            } else {
                slot.state.establish().finish();
                let armed = Self::submit_recv(slot, ud, driver);
                (peeked, armed, slot.rearm_token())
            }
        };
        if failed {
            if let Some(slot) = self.slab.remove_parts(parts) {
                slot.close(driver);
            }
            return ConnectStep::Failed { peeked };
        }
        if !armed {
            self.rearm.queue(rearm);
        }
        ConnectStep::Connected { idx, peeked }
    }
}

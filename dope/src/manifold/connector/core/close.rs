use std::pin::Pin;

use dope_core::backend::Sqe;
use dope_core::driver::submission::Submission;
use dope_core::driver::token::SlotIndex;
use dope_core::driver::token::kind::{CONNECT, SOCKET};
use dope_net::Transport;
use dope_net::link::slot::{PEND_CLOSE, PendingQueue};

use super::{ConnPool, Core};
use crate::DriverContext;
use crate::manifold::connector::app::ConnApp;
use crate::manifold::connector::source::Dialer;
use crate::manifold::env::Env;

pub(super) trait ClosePhase<'d, const ID: u8, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    fn flush_cancellations(self: Pin<&mut Self>);

    fn close_slot(self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>);

    fn drain_close(
        pool: &mut ConnPool<'d, ID, E::Transport, A::Wire, A::Conn, A::Send>,
        dirty: &PendingQueue,
        app: &A,
        idx: SlotIndex,
        driver: &mut DriverContext<'_, 'd>,
    );

    fn maybe_close(self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>);
}

impl<'d, const ID: u8, A, S, E> ClosePhase<'d, ID, A, S, E> for Core<'d, ID, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    fn flush_cancellations(mut self: Pin<&mut Self>) {
        loop {
            let cancel = self.as_ref().project_ref().app.take_cancel();
            let Some((key, idx)) = cancel else {
                break;
            };
            let this = self.as_mut().project();
            let Some(slot) = this.pool.get(idx) else {
                continue;
            };
            if slot.state.dial == key {
                this.dirty.mark(idx, &slot.state.pending, PEND_CLOSE);
            }
        }
    }

    fn close_slot(self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>) {
        let now = driver.turn_now();
        let this = self.project();
        let slot_meta = this.pool.get_mut(idx).and_then(|slot| {
            if slot.state.retired {
                return None;
            }
            slot.state.retired = true;
            let established = slot.state.establish.is_done();
            let key = slot.state.dial;
            let permanent = slot.state.close_permanent;
            if established {
                this.app.close(slot, driver);
            }
            Some((key, permanent))
        });
        if let Some((key, permanent)) = slot_meta {
            if permanent {
                this.upstreams.kill(key);
            } else {
                this.upstreams.disconnect(key, now);
            }
        }
        Self::drain_close(this.pool, this.dirty, this.app, idx, driver);
    }

    fn drain_close(
        pool: &mut ConnPool<'d, ID, E::Transport, A::Wire, A::Conn, A::Send>,
        dirty: &PendingQueue,
        app: &A,
        idx: SlotIndex,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let (send_inflight, establishing, connecting, token) = match pool.get(idx) {
            Some(slot) => (
                slot.core.is_send_inflight(),
                !slot.state.establish.is_done(),
                slot.state.establish.is_connecting(),
                slot.token(),
            ),
            None => return,
        };
        if establishing {
            let op_kind = if connecting { CONNECT } else { SOCKET };
            let cancel = if connecting {
                Sqe::cancel(token, op_kind)
            } else {
                let Some(fd) = pool.fd_of(idx) else {
                    return;
                };
                Sqe::cancel_create(fd.slot())
            };
            let cancelled = driver.push(cancel).is_ok();
            if let Some(slot) = pool.get_mut(idx) {
                slot.core.begin_close();
                if !cancelled {
                    dirty.mark(idx, &slot.state.pending, PEND_CLOSE);
                }
            }
            return;
        }
        if send_inflight {
            if let Some(slot) = pool.get_mut(idx) {
                slot.core.begin_close();
            }
            return;
        }
        if pool
            .get_mut(idx)
            .is_some_and(|slot| slot.seal_graceful(driver, token))
        {
            return;
        }
        let drained = pool
            .get(idx)
            .map(|slot| app.is_drained(slot, driver))
            .unwrap_or(true);
        if drained {
            pool.try_close(idx, driver);
        } else if let Some(slot) = pool.get_mut(idx) {
            dirty.mark(idx, &slot.state.pending, PEND_CLOSE);
        }
    }

    fn maybe_close(mut self: Pin<&mut Self>, idx: SlotIndex, driver: &mut DriverContext<'_, 'd>) {
        let close = {
            let this = self.as_ref().project_ref();
            let Some(slot) = this.pool.get(idx) else {
                return;
            };
            slot.core.should_close(this.app.defer_close(slot, driver))
        };
        if close {
            self.as_mut().close_slot(idx, driver);
        }
    }
}

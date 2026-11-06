use dope_core::driver::{
    receive::credit::{self, error::Arm},
    route::Identity,
};

use crate::{
    link::slot::{
        self,
        recv::{Blocked, Prepared},
        types,
    },
    wire::{
        self,
        receive::{self, mode, reserve},
        terminal,
    },
};

pub(in crate::link) enum Reserve<
    'a,
    'w,
    'd,
    ID: Identity + 'w,
    W: wire::Spec + 'w,
    P: mode::Flag,
    R: reserve::Try<W>,
> where
    'd: 'w,
{
    Blocked(Blocked<'a, 'w, 'd, ID, W, R>),
    Credit(Arm),
    Control(slot::Control),
    Ready(Prepared<'a, 'w, 'd, ID, W, P>),
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, R> Reserve<'a, 'w, 'd, ID, W, mode::Waiting<R>, R>
where
    'd: 'w,
    W: wire::Spec + 'w,
    R: reserve::Try<W>,
    W::Receive: receive::Policy<W, Reserve = reserve::Waiting<R>>,
{
    pub(in crate::link) fn try_new(
        conn: &'a mut types::Control<'w, 'd, ID, W>,
        rt: &'a mut W::Runtime<'w, 'd, ID>,
        credit: credit::Slot<'a, 'd>,
        mut arm: bool,
    ) -> Self {
        let key = &conn.key;
        let end = match conn.engine.both(&mut conn.terminal) {
            terminal::Admission::Open(end) => end,
            terminal::Admission::Closed => {
                return Reserve::Control(slot::Control::Discarded {
                    needs_rearm: arm,
                    finished: false,
                });
            }
        };
        let send = if conn.engine.operations.sending.is_inflight() {
            None
        } else {
            Some(&mut conn.send)
        };
        let txn = match R::try_reserve(&mut conn.wire, send, rt) {
            reserve::Outcome::Ready(txn) => txn,
            reserve::Outcome::Blocked(block) => {
                let credit = match credit
                    .arm(key.target().dispatch(), conn.engine.ready_handle().target())
                {
                    Ok(credit) => credit,
                    Err(cause) => {
                        return Reserve::Credit(cause);
                    }
                };
                conn.engine.operations.recv.pause();
                return Reserve::Blocked(Blocked {
                    block,
                    credit,
                    key,
                    needs_cancel: conn.engine.operations.recv.has_cancel(),
                });
            }
            reserve::Outcome::Failed(cause) => {
                let ((), captured) = end
                    .terminal((), terminal::Terminal::Failed(cause))
                    .into_parts();
                Prepared::<ID, W, mode::Waiting<R>>::capture(&mut conn.engine, &mut arm, captured);
                return Reserve::Control(slot::Control::NoChunk { needs_rearm: arm });
            }
        };
        Reserve::Ready(Prepared {
            engine: &mut conn.engine,
            end,
            transaction: txn,
            credit,
            needs_rearm: arm,
        })
    }
}

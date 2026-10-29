use dope_core::driver::{
    receive::credit::{self, error::Arm},
    route::Identity,
};

use crate::{
    link::{
        pool::recv::Resident,
        slot::{
            self,
            recv::{Blocked, Prepared},
            types,
        },
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
    pub(in crate::link) fn try_new<S>(
        conn: &'a mut types::Connection<'w, 'd, ID, W, S>,
        rt: &'a mut W::Runtime<'w, 'd, ID>,
        credit: credit::Control<'d>,
        mut arm: bool,
    ) -> Self {
        let resident = Resident::new(&conn.key);
        let end = match conn.control.engine.both(&mut conn.control.terminal) {
            terminal::Admission::Open(end) => end,
            terminal::Admission::Closed => {
                return Reserve::Control(slot::Control::Discarded {
                    needs_rearm: arm,
                    finished: false,
                });
            }
        };
        let txn = match R::try_reserve(&mut conn.wire, &mut conn.send, rt) {
            reserve::Outcome::Ready(txn) => txn,
            reserve::Outcome::Blocked(block) => {
                let bind = credit.bind(resident.key().target().dispatch());
                let credit = match <mode::Waiting<R> as mode::Stored>::arm(
                    bind,
                    conn.control.engine.ready_handle().target(),
                ) {
                    Ok(credit) => credit,
                    Err(cause) => {
                        return Reserve::Credit(cause);
                    }
                };
                conn.control.engine.operations.recv.pause();
                return Reserve::Blocked(Blocked {
                    block,
                    credit,
                    resident,
                    needs_cancel: conn.control.engine.operations.recv.has_cancel(),
                });
            }
            reserve::Outcome::Failed(cause) => {
                let ((), captured) = end
                    .terminal((), terminal::Terminal::Failed(cause))
                    .into_parts();
                Prepared::<ID, W, mode::Waiting<R>>::capture(
                    &mut conn.control.engine,
                    &mut arm,
                    captured,
                );
                return Reserve::Control(slot::Control::NoChunk { needs_rearm: arm });
            }
        };
        let credit = credit.bind(resident.key().target().dispatch());
        Reserve::Ready(Prepared {
            engine: &mut conn.control.engine,
            end,
            transaction: txn,
            credit,
            needs_rearm: arm,
        })
    }
}

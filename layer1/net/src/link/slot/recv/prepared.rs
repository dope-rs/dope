use dope_core::{
    driver::route::Identity,
    io::{recv, transfer},
};
use receive::{discard, mode, reserve};

use crate::{
    link::{slot, slot::types},
    wire::{self, receive, terminal},
};

pub(in crate::link) struct Prepared<
    'a,
    'w,
    'd,
    ID: Identity + 'w,
    W: wire::Spec + 'w,
    P: mode::Flag,
> where
    'd: 'w,
{
    pub(in crate::link) engine: &'a mut slot::Engine<'d, ID, W>,
    pub(in crate::link) end: terminal::Both<'a, terminal::Error<'w, W>>,
    pub(in crate::link) transaction: <<<W as wire::Spec>::Receive as receive::Policy<
        W,
    >>::Reserve as reserve::Kind<W>>::Transaction<'a, 'w, 'd, ID>,
    pub(in crate::link) credit: P::Binding<'a, 'd, ID>,
    pub(in crate::link) needs_rearm: bool,
}

impl<'a, 'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w, P: mode::Flag>
    Prepared<'a, 'w, 'd, ID, W, P>
where
    'd: 'w,
{
    pub(in crate::link) fn new(
        conn: &'a mut types::Control<'w, 'd, ID, W>,
        rt: &'a mut W::Runtime<'w, 'd, ID>,
        credit: P::Binding<'a, 'd, ID>,
        arm: bool,
    ) -> Result<Self, slot::Control>
    where
        <W::Receive as receive::Policy<W>>::Reserve: reserve::Immediate<W>,
    {
        let end = match conn.engine.both(&mut conn.terminal) {
            terminal::Admission::Open(end) => end,
            terminal::Admission::Closed => {
                return Err(slot::Control::Discarded {
                    needs_rearm: arm,
                    finished: false,
                });
            }
        };
        Ok(Self {
            engine: &mut conn.engine,
            end,
            transaction:
                <<W::Receive as receive::Policy<W>>::Reserve as reserve::Immediate<W>>::reserve(
                    &mut conn.wire,
                    rt,
                ),
            credit,
            needs_rearm: arm,
        })
    }

    pub(in crate::link) fn capture(
        engine: &mut slot::Engine<'d, ID, W>,
        rearm: &mut bool,
        captured: terminal::Captured,
    ) {
        engine.operations.captured(&engine.establish, captured);
        if let terminal::Captured::Eof | terminal::Captured::Terminal = captured {
            *rearm |= engine.operations.recv.has_cancel();
        }
    }

    pub(in crate::link) fn discard(&mut self, bytes: &mut recv::Lease<'d>) -> (transfer::Len, bool) {
        <<W::Receive as receive::Policy<W>>::Discard as discard::Policy>::consume(
            &mut self.engine.discard,
            bytes,
        )
    }
}

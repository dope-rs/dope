use dope_core::{
    driver::{route::Identity, schedule::app::batch::Bound},
    io::{recv, transfer},
};
use o3::mem;
use receive::{batch, discard, mode, reserve};

use crate::{
    link::{slot, slot::types},
    wire::{
        self, receive,
        receive::{Chunk, mode::Disabled},
        terminal,
    },
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
    pub(in crate::link::slot::recv) engine: &'a mut slot::Engine<'d, ID, W>,
    pub(in crate::link::slot::recv) end: terminal::Both<'a, terminal::Error<'w, W>>,
    pub(in crate::link::slot::recv) transaction: <<<W as wire::Spec>::Receive as receive::Policy<
        W,
    >>::Reserve as reserve::Kind<W>>::Transaction<'a, 'w, 'd, ID>,
    pub(in crate::link::slot::recv) credit: P::Binding<'d, ID>,
    pub(in crate::link::slot::recv) needs_rearm: bool,
}

impl<'a, 'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w, P: mode::Flag>
    Prepared<'a, 'w, 'd, ID, W, P>
where
    'd: 'w,
{
    pub(in crate::link) fn new<S>(
        conn: &'a mut types::Connection<'w, 'd, ID, W, S>,
        rt: &'a mut W::Runtime<'w, 'd, ID>,
        credit: P::Binding<'d, ID>,
        arm: bool,
    ) -> Result<Self, slot::Control>
    where
        <W::Receive as receive::Policy<W>>::Reserve: reserve::Immediate<W>,
    {
        let end = match conn.control.engine.both(&mut conn.control.terminal) {
            terminal::Admission::Open(end) => end,
            terminal::Admission::Closed => {
                return Err(slot::Control::Discarded {
                    needs_rearm: arm,
                    finished: false,
                });
            }
        };
        Ok(Self {
            engine: &mut conn.control.engine,
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

    pub(in crate::link::slot::recv) fn capture(
        engine: &mut slot::Engine<'d, ID, W>,
        rearm: &mut bool,
        captured: terminal::Captured,
    ) {
        engine.captured(captured);
        if let terminal::Captured::Eof | terminal::Captured::Terminal = captured {
            *rearm |= engine.operations.recv.has_cancel();
        }
    }

    fn discard(&mut self, bytes: &mut recv::Lease<'d>) -> (transfer::Len, bool) {
        <<W::Receive as receive::Policy<W>>::Discard as discard::Policy>::consume(
            &mut self.engine.discard,
            bytes,
        )
    }

    pub(in crate::link) fn data<'bytes, 'id>(
        mut self,
        bytes: &'bytes mut recv::Lease<'d>,
        cap: Bound<'id, batch::Capacity<'d, W>>,
    ) -> (
        slot::Decision<batch::Items<'bytes, W>>,
        mem::raw::Unused<'id>,
    )
    where
        'w: 'bytes,
    {
        let (remaining, finished) = self.discard(bytes);
        if remaining.is_zero() {
            let unused = cap.empty();
            return (
                slot::Decision::Control(slot::Control::Discarded {
                    needs_rearm: self.needs_rearm,
                    finished,
                }),
                unused,
            );
        }
        let slice = bytes.as_mut_slice();
        let read = <W::Receive as receive::Policy<W>>::process::<ID>(
            self.transaction,
            slice,
            batch::Grant::new(cap),
            self.end,
        );
        let (produced, captured) = read.into_parts();
        let (chunk, unused) = produced.into_parts();
        Self::capture(self.engine, &mut self.needs_rearm, captured);
        if receive::raw::Batch::len(&chunk) == 0 {
            return (
                slot::Decision::Control(slot::Control::NoChunk {
                    needs_rearm: self.needs_rearm,
                }),
                unused,
            );
        }
        (
            slot::Decision::Chunk {
                chunk: batch::Items::from_raw(chunk),
                needs_rearm: self.needs_rearm,
            },
            unused,
        )
    }

    /// Produces a callback-bounded cursor which cannot install connection
    /// credit; retained ranges own only their budgeted backing storage.
    pub(in crate::link) fn scoped(
        mut self,
        mut bytes: recv::Lease<'d>,
    ) -> slot::Decision<Chunk<'w, 'd, W, Disabled>> {
        let (remaining, finished) = self.discard(&mut bytes);
        if remaining.is_zero() {
            return slot::Decision::Control(slot::Control::Discarded {
                needs_rearm: self.needs_rearm,
                finished,
            });
        }
        let read =
            <W::Receive as receive::Policy<W>>::read_lease::<ID>(self.transaction, bytes, self.end)
                .map(|cursor| cursor.map(Chunk::from_raw));
        let (item, captured) = read.into_parts();
        Self::capture(self.engine, &mut self.needs_rearm, captured);
        match item {
            Some(chunk) => slot::Decision::Chunk {
                chunk,
                needs_rearm: self.needs_rearm,
            },
            None => slot::Decision::Control(slot::Control::NoChunk {
                needs_rearm: self.needs_rearm,
            }),
        }
    }
}

impl<'a, 'w, 'd, ID: Identity + 'w, W, P> Prepared<'a, 'w, 'd, ID, W, P>
where
    'd: 'w,
    W: wire::Spec + 'w,
    W::Receive: receive::Policy<W, Credit = mode::Enabled>,
    P: mode::Stored,
{
    pub(in crate::link) fn retained(
        mut self,
        mut bytes: recv::Lease<'d>,
    ) -> slot::Decision<Chunk<'w, 'd, W>> {
        let (remaining, finished) = self.discard(&mut bytes);
        if remaining.is_zero() {
            return slot::Decision::Control(slot::Control::Discarded {
                needs_rearm: self.needs_rearm,
                finished,
            });
        }
        let credit = match P::arm(self.credit, self.engine.ready_handle().target()) {
            Ok(credit) => credit,
            Err(cause) => {
                return slot::Decision::Control(slot::Control::Credit {
                    cause,
                    needs_rearm: self.needs_rearm,
                });
            }
        };
        let read =
            <W::Receive as receive::Policy<W>>::read_lease::<ID>(self.transaction, bytes, self.end)
                .map(|cursor| cursor.map(Chunk::from_raw));
        let (item, captured) = read.into_parts();
        Self::capture(self.engine, &mut self.needs_rearm, captured);
        match item {
            Some(cursor) => {
                self.engine.operations.recv.pause();
                self.needs_rearm |= self.engine.operations.recv.has_cancel();
                let chunk = cursor.retain(credit.claim().into_return());
                slot::Decision::Chunk {
                    chunk,
                    needs_rearm: self.needs_rearm,
                }
            }
            None => {
                credit.cancel();
                slot::Decision::Control(slot::Control::NoChunk {
                    needs_rearm: self.needs_rearm,
                })
            }
        }
    }
}

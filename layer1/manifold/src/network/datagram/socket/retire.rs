use std::pin;

use dope_core::{
    driver::{
        result,
        route::{Identity, Materialize, kind::Send, table::entries::occupied},
        schedule::{clock, maint},
    },
    io::event::net::send,
};
use o3::{base::cell::region, buffer};

use crate::network::datagram::{
    self, hook,
    send::{Mode, Outcome, Owner, Phase, flights, owners},
    socket::sender::Sender,
};

impl<'d, ID: Identity, B: buffer::Residency + buffer::raw::Source, M: Mode> Sender<'d, ID, B, M> {
    fn retire<R>(
        &mut self,
        done: send::Completion<'d>,
        stale: R,
        matched: impl FnOnce(
            occupied::Entry<'_, flights::Backing<'d, B, M>, <ID as Materialize<Send>>::With>,
            owners::Flight<'d, ID>,
            Outcome,
        ) -> R,
    ) -> Result<R, send::Completion<'d>> {
        let Ok(target) = self.space.parse(done.token()) else {
            return Err(done);
        };
        let Some(entry) = self.in_flight.access().occupied(target.parts()) else {
            return Ok(stale);
        };
        if !entry.get().matches(done.token()) {
            return Err(done);
        }
        Ok(matched(entry, owners::Flight::new(target), done.outcome().into()))
    }

    pub(in crate::network::datagram) fn complete(
        &mut self,
        done: send::Completion<'d>,
    ) -> Result<Option<Outcome>, send::Completion<'d>> {
        self.retire(done, None, |entry, _, outcome| {
            entry.discard();
            Some(outcome)
        })
    }

    pub(in crate::network::datagram) fn settle<H>(
        &mut self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        done: send::Completion<'d>,
        at: clock::Moment<'d>,
        sink: pin::Pin<&mut H>,
    ) -> Result<(), send::Completion<'d>>
    where
        H: datagram::Settlement<'d, ID>,
        H::Hooks: hook::Bundle<Buffer = B>,
    {
        self.retire(done, (), |entry, owner, outcome| {
            let (addr, hold, result) = entry.remove().finish(outcome);
            hold.retire(Owner::Flight(owner), addr, result, at, sink, token);
        })
    }

    pub(in crate::network::datagram) fn drain(
        &mut self,
        work: maint::Work<'_, 'd>,
        mut failed: impl FnMut(result::SubmitError),
    ) {
        self.pending.drain(work, |_, queued| {
            drop(queued);
            if let Phase::Failed(error) = self.phase {
                failed(error);
            }
        });
    }

    pub(in crate::network::datagram) fn drain_event<H>(
        &mut self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        work: maint::Work<'_, 'd>,
        at: clock::Moment<'d>,
        mut sink: pin::Pin<&mut H>,
    ) where
        H: datagram::Settlement<'d, ID>,
        H::Hooks: hook::Bundle<Buffer = B>,
    {
        self.pending.drain(work, |owner, queued| {
            queued.retained.retire(
                Owner::Queued(owner),
                queued.addr,
                self.phase.retirement(),
                at,
                sink.as_mut(),
                token,
            );
        });
    }

    pub(in crate::network::datagram) fn finish(
        &mut self,
        mut failed: impl FnMut(result::SubmitError),
    ) {
        self.phase.stop();
        if let Phase::Failed(error) = self.phase {
            while let Some(queued) = self.pending.pop() {
                drop(queued);
                failed(error);
            }
        } else {
            self.pending.clear();
        }
        let slots = &mut self.in_flight;
        for slot in slots.capacity() {
            let Some(entry) = slots.access().at(slot) else {
                continue;
            };
            entry.discard();
        }
    }

    pub(in crate::network::datagram) fn finish_event<H>(
        &mut self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        at: clock::Moment<'d>,
        mut sink: pin::Pin<&mut H>,
    ) where
        H: datagram::Settlement<'d, ID>,
        H::Hooks: hook::Bundle<Buffer = B>,
    {
        self.phase.stop();
        while let Some((owner, queued)) = self.pending.pop() {
            queued.retained.retire(
                Owner::Queued(owner),
                queued.addr,
                self.phase.retirement(),
                at,
                sink.as_mut(),
                token,
            );
        }
        let slots = &mut self.in_flight;
        let space = self.space;
        for slot in slots.capacity() {
            let Some(entry) = slots.access().at(slot) else {
                continue;
            };
            let key = entry.key();
            let send = entry.remove();
            let owner =
                owners::Flight::new(space.bind(key.slot(), key.epoch()).operation::<Send>());
            let (addr, hold, outcome) = send.finish(Outcome::Shutdown);
            hold.retire(
                Owner::Flight(owner),
                addr,
                outcome,
                at,
                sink.as_mut(),
                token,
            );
        }
    }
}

use std::pin;

use dope_core::{
    driver::{
        result,
        route::{Identity, kind::Send},
        schedule::{clock, maint},
    },
    io::event::net::send,
};
use o3::{base::cell::region, buffer, mem::quota};

use crate::network::datagram::{
    self, hook,
    send::{Mode, Outcome, Owner, Phase, owners},
    socket::sender::Sender,
};

impl<'d, ID: Identity, B: buffer::Residency + buffer::raw::Source, M: Mode> Sender<'d, ID, B, M> {
    pub(in crate::network::datagram) fn complete(
        &mut self,
        done: send::Completion<'d>,
    ) -> Result<Option<Outcome>, send::Completion<'d>> {
        let Ok(target) = self.space.parse(done.token()) else {
            return Err(done);
        };
        let Some(entry) = self.in_flight.access().occupied(target.parts()) else {
            return Ok(None);
        };
        if !entry.get().matches(done.token()) {
            return Err(done);
        }
        entry.discard();
        Ok(Some(done.outcome().into()))
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
        let Ok(target) = self.space.parse(done.token()) else {
            return Err(done);
        };
        let Some(entry) = self.in_flight.access().occupied(target.parts()) else {
            return Ok(());
        };
        if !entry.get().matches(done.token()) {
            return Err(done);
        }
        let owner = owners::Flight::new(target);
        let (addr, hold, result) = match done.outcome() {
            Ok(bytes) => entry.remove().finish(Outcome::Sent(bytes)),
            Err(error) => entry.remove().finish(Outcome::Failed(error)),
        };
        hold.retire(Owner::Flight(owner), addr, result, at, sink, token);
        Ok(())
    }

    pub(in crate::network::datagram) fn drain(
        &mut self,
        work: maint::Work<'_, 'd>,
        mut failed: impl FnMut(result::SubmitError),
    ) {
        while !self.pending.is_empty() {
            if !matches!(
                work.run(|| {
                    drop(self.pending.pop()?);
                    if let Phase::Failed(error) = self.phase {
                        failed(error);
                    }
                    Some(())
                }),
                quota::Admission::Item(())
            ) {
                break;
            }
        }
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
        while !self.pending.is_empty() {
            match work.run(|| {
                let (owner, queued) = self.pending.pop()?;
                queued.retained.retire(
                    Owner::Queued(owner),
                    queued.addr,
                    self.phase.retirement(),
                    at,
                    sink.as_mut(),
                    token,
                );
                Some(())
            }) {
                quota::Admission::Item(()) => {}
                quota::Admission::Empty | quota::Admission::Exhausted => break,
            }
        }
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

use o3::buffer;

use crate::conn::stream::state::transmit::{log, peer, queue};
use crate::conn::{
    self,
    machine::storage::capacity,
    output::{control, delivery},
    stream::state::{resident::Table, streams},
};
use crate::proto::param;
use crate::wire::{
    stream::{Id, Initiator},
    varint::VarInt,
};
use log::journal;
use std::num::NonZero;

pub(in crate::conn) struct State<D: buffer::raw::Segment> {
    pub(in crate::conn::stream::state::transmit) scratch_pending: Vec<queue::Handle>,
    pub(in crate::conn::stream::state::transmit) scratch_start: u8,
    pub(in crate::conn::stream::state::transmit) scratch_consumed: bool,
    schedule_phase: u8,
    schedule_remaining: u32,
    pub(in crate::conn::stream::state::transmit) deliveries: journal::Journal,
    pub(in crate::conn::stream::state::transmit) map: queue::Map<D>,
    pub(in crate::conn) peer: peer::Peer,
}

impl<D: buffer::raw::Segment> State<D> {
    pub(in crate::conn) fn new(
        mut scratch: Vec<queue::Handle>,
        map: queue::Map<D>,
        journal: capacity::Capacity<capacity::Stream>,
    ) -> Self {
        scratch.clear();
        Self {
            scratch_pending: scratch,
            scratch_start: 0,
            scratch_consumed: false,
            schedule_phase: 0,
            schedule_remaining: 0,
            deliveries: journal::Journal::new(journal),
            map,
            peer: peer::Peer::new(queue::Credit::new(VarInt::ZERO)),
        }
    }

    pub(in crate::conn) fn map(&self) -> &queue::Map<D> {
        &self.map
    }

    pub(in crate::conn) fn deliveries(&self) -> &journal::Journal {
        &self.deliveries
    }

    pub(in crate::conn) fn has_snapshot(&self) -> bool {
        !self.scratch_pending.is_empty()
    }

    /// A retained window owns one bounded cleanup turn after its last packet.
    pub(in crate::conn) fn has_work(&self, control: &control::Pending, can_control: bool) -> bool {
        self.schedule_remaining != 0
            || !self.scratch_pending.is_empty()
            || self.map.has_work(control, can_control)
    }

    pub(in crate::conn) fn wake_data(&mut self) {
        self.map.wake(queue::DATA);
    }

    pub(in crate::conn) fn retry_data(&mut self) {
        self.map.wake(queue::DATA | queue::ADVISORY);
    }

    pub(in crate::conn) fn wake_streams(&mut self, direction: crate::wire::stream::Direction) {
        self.map.wake(queue::stream_wait(direction));
    }

    pub(in crate::conn) fn wake_stream(&mut self, id: Id, registry: &Table) {
        if let Some((_, mut occupied)) = self.map.bind(id, registry) {
            occupied.refresh(true);
        }
    }

    pub(in crate::conn) fn snapshot(
        &mut self,
        control: &mut control::Pending,
        work: usize,
        can_control: bool,
        admit: impl Fn(Id) -> bool,
    ) -> Result<(), control::Admission> {
        const {
            assert!(conn::STREAM_SCHEDULE_WORK_LIMIT <= u8::MAX as usize + 1);
        }
        if !self.scratch_consumed && !self.scratch_pending.is_empty() {
            return Ok(());
        }
        self.map
            .retain(&self.scratch_pending, &self.peer, control, can_control);
        self.scratch_consumed = false;
        self.scratch_start = 0;
        let result = self.map.snapshot(
            &mut self.scratch_pending,
            &mut self.schedule_remaining,
            control,
            &self.peer,
            work,
            can_control,
            admit,
        );
        if let Some(len) = NonZero::new(self.scratch_pending.len()) {
            self.scratch_start = (usize::from(self.schedule_phase) % len.get()) as u8;
        }
        if self.schedule_remaining == 0 {
            self.schedule_phase = if self.map.view().has_pending() {
                self.schedule_phase.wrapping_add(1)
            } else {
                0
            };
        }
        result
    }

    pub(in crate::conn) fn selection(&mut self) -> (log::Selection<'_>, &mut queue::Map<D>) {
        (log::Selection::new(&mut self.deliveries), &mut self.map)
    }

    pub(in crate::conn) fn set_credit(
        &mut self,
        peer: &param::Set,
        local: Initiator,
        early: bool,
        control: &mut control::Pending,
    ) -> Result<(), conn::Error> {
        self.scratch_pending.clear();
        self.map.try_for_each::<conn::Error>(|mut occupied| {
            let relative = streams::Relative::new(occupied.id(), local);
            let limit = queue::Entry::limit(Some(peer), relative);
            let credit = occupied.credit_mut();
            if early {
                credit.raise(limit, control);
            } else {
                credit.replace(limit, control);
            }
            occupied.refresh(true);
            Ok(())
        })
    }

    pub(in crate::conn) fn lose(&mut self, handle: delivery::Handle<delivery::Stream>) {
        self.deliveries.lose(handle);
    }

    pub(in crate::conn) fn arm_probes(&mut self) {
        self.deliveries.arm_probes();
    }

    pub(in crate::conn) fn reject_early(
        &mut self,
        control: &mut control::Pending,
    ) -> Result<(), conn::Error> {
        self.map.try_for_each::<conn::Error>(|mut occupied| {
            occupied.reject_early(&mut self.deliveries, control)?;
            occupied.refresh(true);
            Ok(())
        })?;
        self.peer.reject_early();
        Ok(())
    }
}

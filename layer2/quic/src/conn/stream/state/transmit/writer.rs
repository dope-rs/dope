use crate::stream::release;

use o3::buffer::{self, resident::Shared};

use std::{convert, mem};

use crate::errors::detail;
use crate::wire::{
    stream::{Direction, Id, Initiator},
    varint,
};
use crate::{conn, stream};
use conn::stream::{
    Access, Error, SendOutcome, Streams, abandon, resident,
    state::{resident::Table, streams, transmit},
};
use send::admit;
use stream::send;
use transmit::queue;

/// The single unbranded send kernel used by the branded access API.
#[repr(transparent)]
pub(in crate::conn) struct Writer<'conn, const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment>
{
    connection: &'conn mut conn::Machine<DOMAIN, B, D>,
}

impl<'conn, const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment>
    Writer<'conn, DOMAIN, B, D>
{
    pub(in crate::conn) fn new(machine: &'conn mut conn::Machine<DOMAIN, B, D>) -> Self {
        Self {
            connection: machine,
        }
    }

    pub(in crate::conn) fn is_available(machine: &conn::Machine<DOMAIN, B, D>) -> bool {
        (machine.egress.lifecycle.state == conn::State::Established
            || matches!(machine.peer.local, Initiator::Client)
                && machine.egress.lifecycle.state == conn::State::Handshaking
                && machine.handshake.keys.long.early().is_some()
                && machine.peer.transport_params.is_some())
            && !machine.is_closing()
    }

    pub(in crate::conn) fn open(&mut self, dir: Direction) -> Result<resident::Local, Error> {
        let ready = Self::is_available(self.connection);
        transmit::Sender::new(&mut self.connection.streams).open_local(
            dir,
            self.connection.peer.transport_params.as_ref(),
            ready,
        )
    }

    pub(in crate::conn) fn send(&mut self, id: Id, data: &[u8]) -> Result<(), Error> {
        let ready = Self::is_available(self.connection);
        transmit::Sender::new(&mut self.connection.streams).send_bytes(
            id,
            data,
            self.connection.peer.transport_params.as_ref(),
            ready,
        )
    }

    pub(in crate::conn) fn try_buffer(
        &mut self,
        id: Id,
        data: send::Buffer<D>,
    ) -> SendOutcome<send::Buffer<D>> {
        let ready = Self::is_available(self.connection);
        transmit::Sender::new(&mut self.connection.streams).send_batch(
            id,
            data,
            self.connection.peer.transport_params.as_ref(),
            ready,
        )
    }

    pub(in crate::conn) fn try_parts(
        &mut self,
        id: Id,
        parts: send::Parts<D>,
    ) -> SendOutcome<send::Parts<D>> {
        let ready = Self::is_available(self.connection);
        transmit::Sender::new(&mut self.connection.streams).send_batch(
            id,
            parts,
            self.connection.peer.transport_params.as_ref(),
            ready,
        )
    }

    pub(in crate::conn) fn try_admit<C, W>(
        &mut self,
        id: Id,
        owner: W,
        commit: C,
    ) -> admit::Outcome<D, C, Error, W>
    where
        C: admit::Commit<D, W>,
        W: send::raw::Batch<D>,
    {
        let ready = Self::is_available(self.connection);
        transmit::Sender::new(&mut self.connection.streams).send_with(
            id,
            owner,
            self.connection.peer.transport_params.as_ref(),
            ready,
            commit,
        )
    }

    pub(in crate::conn) fn try_local<C, W>(
        &mut self,
        resident: resident::Local,
        owner: W,
        commit: C,
    ) -> admit::Outcome<D, C, resident::Error, W>
    where
        C: admit::Commit<D, W>,
        W: send::raw::Batch<D>,
    {
        let ready = Self::is_available(self.connection);
        transmit::Sender::new(&mut self.connection.streams).try_send(
            resident,
            owner,
            self.connection.peer.transport_params.as_ref(),
            ready,
            commit,
        )
    }

    pub(in crate::conn) fn try_prepare(
        &mut self,
        resident: resident::Local,
    ) -> Result<(), resident::Error> {
        let ready = Self::is_available(self.connection);
        transmit::Sender::new(&mut self.connection.streams).try_prepare(
            resident,
            self.connection.peer.transport_params.as_ref(),
            ready,
        )
    }

    pub(in crate::conn) fn finish(&mut self, id: Id) -> Result<(), Error> {
        let ready = Self::is_available(self.connection);
        transmit::Sender::new(&mut self.connection.streams).send_fin(
            id,
            self.connection.peer.transport_params.as_ref(),
            ready,
        )
    }

    fn reset<E: release::Sink, F>(
        &mut self,
        id: Id,
        code: crate::errors::Code,
        sink: &mut E,
        select: impl for<'map, 'table> FnOnce(
            &'map mut queue::Map<D>,
            &'table mut Table,
            varint::VarInt,
            queue::Preparation,
        ) -> Result<Option<queue::Occupied<'map, D>>, F>,
        map_error: impl FnOnce(Error) -> F,
    ) -> Result<(), F> {
        let ready = Self::is_available(self.connection);
        let sender = transmit::Sender::new(&mut self.connection.streams);
        sender
            .validate(id, Access::Send, ready)
            .map_err(map_error)?;
        if sender.is_send_done(id) {
            return Ok(());
        }
        let stream = streams::Relative::new(id, sender.local());
        let credit = queue::Entry::limit(self.connection.peer.transport_params.as_ref(), stream);
        let Streams { state, events } = &mut *sender.streams;
        let (slots, tx) = (&mut state.resident, &mut state.transmit);
        let (log, map) = (&mut tx.deliveries, &mut tx.map);
        let Some(mut slot) = select(map, slots, credit, queue::Preparation::new(stream))? else {
            return Ok(());
        };
        let Some(freed) = slot.reset::<E>(log, &mut self.connection.control, code) else {
            return Ok(());
        };
        freed.commit(release::Cause::Reset, events, sink);
        slot.refresh(true);
        Ok(())
    }

    pub(in crate::conn) fn reset_with<E: release::Sink>(
        &mut self,
        id: Id,
        code: crate::errors::Code,
        sink: &mut E,
    ) -> Result<(), Error> {
        self.reset(
            id,
            code,
            sink,
            |map, table, credit, mode| {
                map.try_prepared(queue::Id::new(id), None, credit, mode, table, |_, slot| {
                    slot
                })
            },
            convert::identity,
        )
    }

    pub(in crate::conn) fn abandon_with<E: release::Sink>(
        &mut self,
        id: Id,
        code: crate::errors::Code,
        sink: &mut E,
    ) -> Result<(), abandon::Error> {
        if !Self::is_available(self.connection) {
            return Err(abandon::Error::Stream(Error::NotEstablished));
        }
        let conn::Machine {
            streams,
            control,
            peer,
            ..
        } = &mut *self.connection;
        let impact = transmit::Commit::new(&mut streams.state)
            .plan_stop(id)
            .map_err(|error| abandon::Error::Plan(error.into()))?;
        if !impact.active {
            return Ok(());
        }
        detail::Plan::try_capacity(
            detail::Slot::Send,
            impact.stream_slots,
            streams.transmit.map().view().vacancy(),
        )
        .map_err(abandon::Error::Plan)?;
        detail::Plan::try_capacity(
            detail::Slot::Resident,
            usize::from(impact.key.is_none()),
            streams.resident.available(),
        )
        .map_err(abandon::Error::Plan)?;
        let mut events = streams
            .events
            .try_reserve(impact.event_slots)
            .map_err(abandon::Error::Events)?;
        transmit::Commit::new(&mut streams.state)
            .try_stop(
                impact.key,
                id,
                code,
                peer.transport_params.as_ref(),
                transmit::Effects::new(control, &mut events, sink),
            )
            .map_err(abandon::Error::Stream)
    }

    pub(in crate::conn) fn try_reset<E: release::Sink>(
        &mut self,
        local: resident::Local,
        code: crate::errors::Code,
        sink: &mut E,
    ) -> Result<(), resident::Error> {
        self.reset(
            local.id(),
            code,
            sink,
            |map, table, credit, mode| map.try_live(local, credit, mode, table),
            resident::Error::Stream,
        )
    }
}

const _: () = {
    assert!(
        mem::size_of::<Writer<'static, 0, Vec<u8>, Shared<'static>>>()
            == mem::size_of::<&mut conn::Machine<0, Vec<u8>, Shared<'static>>>()
    );
    assert!(
        mem::align_of::<Writer<'static, 0, Vec<u8>, Shared<'static>>>()
            == mem::align_of::<&mut conn::Machine<0, Vec<u8>, Shared<'static>>>()
    );
};

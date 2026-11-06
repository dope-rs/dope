use crate::stream::{
    outbound,
    send::{self, admit},
};

use std::ops;

use crate::conn::output::control::{self, kind};
use crate::conn::stream::resident::slot;
use crate::conn::stream::state::streams;
use crate::conn::stream::state::transmit::{
    log, peer,
    queue::{self, Phase},
};
use crate::proto::param;
use crate::wire::stream::Direction;
use crate::wire::varint;
use control::owner;

/// Mutable state for one live stream send queue.
pub(in crate::conn) struct Entry {
    pub(super) stream: outbound::Sender,
    pub(super) credit: queue::Credit<kind::StreamBlock>,
    pub(super) delivery_group: Option<log::GroupId>,
    pub(super) reset_stream: owner::Signal<kind::ResetStream>,
}

impl Entry {
    pub(super) fn work(
        &self,
        peer: &peer::Peer,
        control: &control::Pending,
        can_control: bool,
    ) -> queue::Work {
        if self.is_deferred() {
            return if !can_control {
                queue::Work::Blocked(0)
            } else if control.available().raw() != 0 {
                queue::Work::Ready
            } else {
                queue::Work::Control(queue::CONTROL)
            };
        }
        let view = self.view();
        if !view.has_pending() || !matches!(self.phase(), Phase::Open | Phase::Marked) {
            return queue::Work::Blocked(0);
        }
        if view.unsent_len() == 0 && matches!(view.boundary(0), send::Boundary::End) {
            return queue::Work::Ready;
        }
        let budget = peer.budget(0);
        let credit = self
            .credit
            .limit()
            .get()
            .saturating_sub(view.next_offset().get());
        if budget != 0 && credit != 0 {
            return queue::Work::Ready;
        }
        let global = if budget == 0 {
            queue::ADVISORY | if credit != 0 { queue::DATA } else { 0 }
        } else {
            0
        };
        if !can_control {
            return queue::Work::Blocked(global);
        }
        let ready = control.ready();
        if (budget == 0 && ready.has_blocked(&peer.data_credit))
            || (credit == 0 && ready.has_blocked(&self.credit))
        {
            queue::Work::Ready
        } else if (budget == 0 && peer.data_credit.blocked.is_none())
            || (credit == 0 && self.credit.blocked.is_none())
        {
            queue::Work::Control(queue::CONTROL | global)
        } else {
            queue::Work::Blocked(global)
        }
    }

    pub(in crate::conn) fn limit(
        peer: Option<&param::Set>,
        stream: streams::Relative,
    ) -> varint::VarInt {
        let Some(peer) = peer else {
            return varint::VarInt::ZERO;
        };
        let flow = peer.flow();
        match (stream.direction(), stream.is_local()) {
            (Direction::Uni, true) => flow.stream_uni(),
            (Direction::Uni, false) => varint::VarInt::ZERO,
            (Direction::Bidi, true) => flow.remote_bidi(),
            (Direction::Bidi, false) => flow.local_bidi(),
        }
    }

    pub(in crate::conn) fn credit(&self) -> &queue::Credit<kind::StreamBlock> {
        &self.credit
    }

    pub(in crate::conn) fn is_deferred(&self) -> bool {
        self.reset_stream.is_deferred()
    }

    pub(super) fn deferred(
        &mut self,
    ) -> Option<(owner::Deferred<'_, kind::ResetStream>, varint::VarInt)> {
        Some((
            self.reset_stream.deferred()?,
            self.stream.view().next_offset(),
        ))
    }

    pub(in crate::conn) fn is_reset(&self) -> bool {
        !self.reset_stream.is_empty()
    }

    pub(in crate::conn) fn phase(&self) -> Phase {
        if self.is_reset() {
            return Phase::Reset;
        }
        match self.stream.finish() {
            outbound::Finish::More => Phase::Open,
            outbound::Finish::Marked => Phase::Marked,
            outbound::Finish::Sent => Phase::Sent,
            outbound::Finish::Acked => Phase::Acked,
        }
    }

    pub(super) fn open(&mut self) -> Result<outbound::Open<'_>, admit::Cause> {
        if self.is_reset() {
            return Err(admit::Cause::Reset);
        }
        self.stream.open().ok_or(admit::Cause::Finished)
    }
}

impl slot::Reusable for Entry {
    type Init = varint::VarInt;

    fn new(init: Self::Init) -> Self {
        Self {
            stream: outbound::Sender::default(),
            credit: queue::Credit::new(init),
            delivery_group: None,
            reset_stream: owner::Signal::new(),
        }
    }

    fn reuse(&mut self, init: Self::Init) {
        self.credit = queue::Credit::new(init);
    }
}

impl ops::Deref for Entry {
    type Target = outbound::Sender;

    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

const _: () = assert!(
    std::mem::size_of::<Entry>()
        == std::mem::size_of::<outbound::Sender>() + 4 * std::mem::size_of::<u64>()
);

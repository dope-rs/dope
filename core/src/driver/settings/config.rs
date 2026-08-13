use std::io;

use crate::driver::settings;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    queues: settings::QueueLayout,
    scheduler: settings::SchedulerLayout,
    file_slots: settings::FileSlots,
    recv: settings::Receive,
    completion_progress: settings::CompletionProgress,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            queues: settings::QueueLayout::fixed::<1024, 2048>(),
            scheduler: settings::SchedulerLayout::fixed::<65_536, 65_536>(),
            file_slots: settings::FileSlots::fixed::<0, 256>(),
            recv: settings::Receive::fixed::<128, 4096>(),
            completion_progress: settings::CompletionProgress::Prompt,
        }
    }
}

impl Config {
    fn invalid(message: &'static str) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, message)
    }

    #[must_use]
    pub const fn file_slots(&self) -> settings::FileSlots {
        self.file_slots
    }

    #[must_use]
    pub const fn queue_layout(&self) -> settings::QueueLayout {
        self.queues
    }

    #[must_use]
    pub const fn scheduler(&self) -> settings::SchedulerLayout {
        self.scheduler
    }

    #[must_use]
    pub const fn completion_progress(&self) -> settings::CompletionProgress {
        self.completion_progress
    }

    #[must_use]
    pub const fn with_completion_progress(
        mut self,
        completion_progress: settings::CompletionProgress,
    ) -> Self {
        self.completion_progress = completion_progress;
        self
    }

    #[must_use]
    pub const fn with_file_slots(mut self, file_slots: settings::FileSlots) -> Self {
        self.file_slots = file_slots;
        self
    }

    #[must_use]
    pub const fn receive(&self) -> settings::Receive {
        self.recv
    }

    #[must_use]
    pub const fn with_queue_layout(mut self, queues: settings::QueueLayout) -> Self {
        self.queues = queues;
        self
    }

    #[must_use]
    pub const fn with_scheduler(mut self, scheduler: settings::SchedulerLayout) -> Self {
        self.scheduler = scheduler;
        self
    }

    pub fn for_profile<P: settings::Profile>() -> io::Result<Self> {
        let file_slots = settings::FileSlots::new(0, P::OUTBOUND_SLOTS)
            .ok_or_else(|| Self::invalid("dope: profile fixed-file layout is invalid"))?;
        Ok(Self {
            queues: P::QUEUES,
            scheduler: P::SCHEDULER,
            file_slots,
            recv: P::RECEIVE,
            completion_progress: P::COMPLETION_PROGRESS,
        })
    }

    pub fn for_tcp_profile<P: settings::Profile>(max_connections: usize) -> io::Result<Self> {
        let accept_slots = u32::try_from(max_connections.min(P::MAX_ACCEPT_SLOTS as usize))
            .map_err(|_| Self::invalid("dope: connection count exceeds u32"))?;
        let file_slots = settings::FileSlots::new(accept_slots, P::OUTBOUND_SLOTS)
            .ok_or_else(|| Self::invalid("dope: profile fixed-file layout is invalid"))?;
        Ok(Self {
            queues: P::QUEUES,
            scheduler: P::SCHEDULER,
            file_slots,
            recv: P::RECEIVE,
            completion_progress: P::COMPLETION_PROGRESS,
        })
    }

    /// Creates a UDP layout from usable payload bytes, excluding backend framing.
    pub fn for_quic_udp(recv_buf_entries: u32, recv_payload_len: u32) -> io::Result<Self> {
        let entries = u16::try_from(recv_buf_entries)
            .map_err(|_| Self::invalid("dope: receive buffer entries exceed u16"))?;
        let recv = settings::Receive::for_datagram_payload(entries, recv_payload_len)
            .ok_or_else(|| Self::invalid("dope: invalid receive buffer layout"))?;
        Ok(Self {
            queues: settings::QueueLayout::fixed::<256, 1024>(),
            scheduler: settings::SchedulerLayout::DEFAULT,
            file_slots: settings::FileSlots::fixed::<0, 16>(),
            recv,
            completion_progress: settings::CompletionProgress::Prompt,
        })
    }

    #[must_use]
    pub fn with_receive(mut self, receive: settings::Receive) -> Self {
        self.recv = receive;
        self
    }

    /// Validates representation relationships required before driver allocation.
    pub(crate) fn validate_structure(&self) -> io::Result<()> {
        let file_slots = self.file_slots;
        let ready_capacity = (file_slots.capacity() as usize)
            .checked_add(self.scheduler.ready().get())
            .ok_or_else(|| Self::invalid("dope: total ready capacity overflow"))?;
        if ready_capacity > u32::MAX as usize {
            return Err(Self::invalid("dope: total ready capacity exceeds u32"));
        }
        Ok(())
    }
}

const _: () = {
    assert!(std::mem::size_of::<Config>() == std::mem::size_of::<[u32; 9]>());
    assert!(std::mem::align_of::<Config>() == std::mem::align_of::<u32>());
};

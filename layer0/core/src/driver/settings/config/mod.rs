use crate::driver::{settings, settings::errors};

mod layout;
pub(super) mod plan;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Config {
    queues: settings::QueueLayout,
    schedule: settings::Schedule,
    files: settings::FileSlots,
    recv: settings::Receive,
    domains: settings::Domain,
    progress: settings::Progress,
}

impl Default for Config {
    fn default() -> Self {
        const FILES: settings::FileSlots = settings::FileSlots::fixed::<0, 256>();
        Self::new(
            settings::QueueLayout::fixed::<1024, 2048>(),
            settings::Schedule::fixed::<{ u16::MAX as u32 - FILES.capacity().raw() }, 65_536>(),
            FILES,
            settings::Receive::fixed::<128, 4096>(),
            settings::Domain::ZERO,
            settings::Progress::Prompt,
        )
    }
}

impl Config {
    /// Assembles validated fields; driver creation checks their relationships.
    pub const fn new(
        queues: settings::QueueLayout,
        schedule: settings::Schedule,
        files: settings::FileSlots,
        recv: settings::Receive,
        domains: settings::Domain,
        progress: settings::Progress,
    ) -> Self {
        Self {
            queues,
            schedule,
            files,
            recv,
            domains,
            progress,
        }
    }

    #[must_use]
    pub const fn file_slots(&self) -> settings::FileSlots {
        self.files
    }

    #[must_use]
    pub const fn queue_layout(&self) -> settings::QueueLayout {
        self.queues
    }

    #[must_use]
    pub const fn schedule(&self) -> settings::Schedule {
        self.schedule
    }

    #[must_use]
    pub const fn progress(&self) -> settings::Progress {
        self.progress
    }

    #[must_use]
    pub const fn with_files(mut self, files: settings::FileSlots) -> Self {
        self.files = files;
        self
    }

    #[must_use]
    pub const fn receive(&self) -> settings::Receive {
        self.recv
    }

    /// Number of stable resident-accounting domains provisioned for one
    /// generative driver scope.
    #[must_use]
    pub const fn domains(&self) -> settings::Domain {
        self.domains
    }

    #[must_use]
    pub const fn with_domains(mut self, domains: settings::Domain) -> Self {
        self.domains = domains;
        self
    }

    #[must_use]
    pub const fn with_queues(mut self, queues: settings::QueueLayout) -> Self {
        self.queues = queues;
        self
    }

    #[must_use]
    pub const fn set_schedule(mut self, schedule: settings::Schedule) -> Self {
        self.schedule = schedule;
        self
    }

    /// Materializes a statically validated profile baseline.
    #[must_use]
    pub const fn for_profile<P: settings::Profile>() -> Self {
        const {
            let config = Self::new(
                P::QUEUES,
                P::SCHEDULER,
                P::FILES,
                P::RECEIVE,
                P::RESIDENT_DOMAINS,
                P::COMPLETION_PROGRESS,
            );
            assert!(plan::Plan::new(config).is_ok(), "capacity");
            config
        }
    }

    #[must_use]
    pub fn with_receive(mut self, receive: settings::Receive) -> Self {
        self.recv = receive;
        self
    }

    /// Validates cross-field relationships before any driver allocation or I/O.
    pub(crate) fn into_plan(self) -> Result<settings::Plan, errors::Config> {
        settings::Plan::new(self)
    }
}

const _: () = {
    assert!(std::mem::align_of::<Config>() == std::mem::align_of::<u32>());
};

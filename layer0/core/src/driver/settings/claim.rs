use crate::driver::{
    settings,
    settings::{demands, errors},
};

/// Hard resource demand of one statically installed component.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Claim {
    routes: settings::Routes,
    files: demands::Files,
    receive: demands::Receive,
    resident_domains: settings::Domain,
    schedule: demands::Schedule,
}

impl Claim {
    pub const fn new(routes: settings::Routes) -> Self {
        Self {
            routes,
            files: demands::Files::ZERO,
            receive: demands::Receive::ZERO,
            resident_domains: settings::Domain::ZERO,
            schedule: demands::Schedule::ZERO,
        }
    }

    /// Adds descriptor demand for accepted and outbound domains.
    #[must_use]
    pub const fn files(mut self, files: demands::Files) -> Self {
        self.files = files;
        self
    }

    /// Adds cross-backend receive buffers including backend framing.
    #[must_use]
    pub const fn receive(mut self, receive: demands::Receive) -> Self {
        self.receive = receive;
        self
    }

    /// Adds stable resident domains for retained byte budgets.
    #[must_use]
    pub const fn domains(mut self, domains: settings::Domain) -> Self {
        self.resident_domains = domains;
        self
    }

    #[must_use]
    pub const fn schedule(mut self, schedule: demands::Schedule) -> Self {
        self.schedule = schedule;
        self
    }
}

/// Accumulated hard requirements for one exact driver domain.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
#[repr(transparent)]
pub struct Needs {
    claim: Claim,
}

const _: () = {
    assert!(std::mem::size_of::<Needs>() == std::mem::size_of::<Claim>());
    assert!(std::mem::align_of::<Needs>() == std::mem::align_of::<Claim>());
    assert!(!std::mem::needs_drop::<Needs>());
};

impl Needs {
    pub const fn new() -> Self {
        Self {
            claim: Claim::new(settings::Routes::EMPTY),
        }
    }

    pub fn include(mut self, next: Claim) -> Result<Self, errors::Claim> {
        if let Some(route) = self.claim.routes.overlap(next.routes) {
            return Err(errors::Claim::DuplicateRoute(route));
        }
        let mut merged = self.claim;
        merged.routes = merged.routes.join(next.routes);
        merged.files = merged.files.merge(next.files)?;
        merged.receive = merged.receive.merge(next.receive)?;
        merged.resident_domains = merged.resident_domains.combine(next.resident_domains)?;
        merged.schedule = merged.schedule.merge(next.schedule)?;
        self.claim = merged;
        Ok(self)
    }

    /// Builds and validates a complete typed resource tree in one operation.
    pub fn from_tree(tree: &impl settings::Resource) -> Result<Self, errors::Claim> {
        tree.include_in(Self::new())
    }

    /// Resolves hard requirements against a profile baseline. Component
    /// demands can enlarge, but never silently shrink, the selected profile.
    pub fn resolve<P: settings::Profile>(&self) -> Result<settings::Config, errors::Claim> {
        self.apply_to(settings::Config::for_profile::<P>())
    }

    /// Resolves a fully controlled topology using its declared receive pool
    /// rather than the profile floor. This decision belongs to the topology,
    /// never to an independently reusable component claim.
    pub fn exact<P: settings::Profile>(&self) -> Result<settings::Config, errors::Claim> {
        self.apply_exact(settings::Config::for_profile::<P>())
    }

    /// Applies component requirements to a caller-supplied baseline. This is
    /// used by process runtimes that add protocol listeners to a larger custom
    /// driver layout without discarding either side's resource contract.
    pub fn apply_to(&self, baseline: settings::Config) -> Result<settings::Config, errors::Claim> {
        let receive = baseline.receive().resolve(self.claim.receive)?;
        self.apply(baseline, receive)
    }

    pub fn apply_exact(
        &self,
        baseline: settings::Config,
    ) -> Result<settings::Config, errors::Claim> {
        let receive = settings::Receive::exact(self.claim.receive)?;
        self.apply(baseline, receive)
    }

    fn apply(
        &self,
        baseline: settings::Config,
        receive: settings::Receive,
    ) -> Result<settings::Config, errors::Claim> {
        let base = baseline.file_slots();
        let files = base.resolve(self.claim.files)?;

        let base = baseline.schedule();
        let schedule = base.resolve(self.claim.schedule);

        let domains = baseline.domains().resolve(self.claim.resident_domains);

        Ok(baseline
            .with_files(files)
            .set_schedule(schedule)
            .with_receive(receive)
            .with_domains(domains))
    }
}

use std::{io, os::fd};

use crate::{
    backend::{
        self,
        uring::{
            engine::{lifecycle, submit, tuning},
            ring,
        },
    },
    driver::{self, lifecycle::routing, settings},
    platform,
};

impl platform::Runtime for backend::Uring {
    fn build(config: &settings::Config) -> io::Result<Self> {
        use crate::backend::fixed;

        let file_slots = config.file_slots();
        let candidate = ring::Candidate::build(config)?;
        let ring = ring::Admissions::new(candidate, file_slots).admit()?;
        let fixed_slots = fixed::Slots::new(file_slots)?;
        let lifecycle = lifecycle::Table::new(file_slots.table_capacity().get())?;
        let tuning = tuning::Table::new(file_slots.table_capacity())?;
        Ok(backend::Uring {
            ring,
            tuning,
            lifecycle,
            reactor_cursor: false,
            fixed_slots,
            routes: routing::Routes::new(),
        })
    }

    fn register_shutdown(&mut self, source: driver::Source<'_>) -> io::Result<()> {
        use std::io::Error;

        use crate::backend::uring::submission;
        let fd = source.into_fd();
        let poll = submission::Submission::poll_shutdown(fd::AsRawFd::as_raw_fd(&fd));
        submit::Writer::new(&mut self.ring)
            .submit(&poll)
            .map_err(Error::from)?;
        self.ring.submit().map(|_| ())
    }
}

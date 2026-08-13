use std::io;

use crate::{backend, backend::uring::capabilities::lifecycle::terminal, driver::flight, platform};

impl platform::Quiesce for backend::Uring {
    fn all(&mut self, drain: flight::Drain<'_, '_>) -> io::Result<()> {
        terminal::Terminal::<terminal::Cancelled>::new(self, drain)
            .cancel()?
            .settle()
    }
}

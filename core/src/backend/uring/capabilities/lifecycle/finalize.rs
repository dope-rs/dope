use std::io;

use crate::{
    backend::{self, fixed, uring::capabilities::lifecycle::terminal},
    driver::flight,
};

impl fixed::Finalize for backend::Uring {
    fn settle<'q, 'd>(&mut self, drain: flight::Drain<'q, 'd>) -> io::Result<()> {
        terminal::Terminal::<terminal::Closing>::new(self, drain).settle()?;
        if self.has_maintenance() || !self.tuning.is_quiescent() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "dope-uring: final settlement left live backend state",
            ));
        }
        Ok(())
    }
}

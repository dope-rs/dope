use std::mem;

use crate::{
    process::{self, failure::bus, runtime::bus::Tx},
    shutdown,
};

/// Consumes a shutdown result at the final worker failure boundary.
pub struct Terminal<'run> {
    pub(in crate::process) source: Tx<'run>,
}

impl<'run> Terminal<'run> {
    pub fn fail(self) -> Result<bus::Event, bus::Fault> {
        match self.source.report() {
            bus::Report::Clean => Ok(bus::Event::Failed),
            bus::Report::Failed(cause) => Err(cause),
        }
    }

    pub fn finish(
        self,
        result: shutdown::Requested<process::Shutdown<'run>>,
    ) -> Result<bus::Event, bus::Fault> {
        result.terminal(self.source.report())
    }
}

impl<'run> shutdown::Requested<process::Shutdown<'run>> {
    pub(crate) fn terminal(self, report: bus::Report) -> Result<bus::Event, bus::Fault> {
        match report {
            bus::Report::Clean => Ok(bus::Event::Done),
            bus::Report::Failed(failure) => Err(failure),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<shutdown::Requested<process::Shutdown<'static>>>() == 0);
    assert!(mem::size_of::<Terminal<'static>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Terminal<'static>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Option<Terminal<'static>>>() == mem::size_of::<usize>());
};

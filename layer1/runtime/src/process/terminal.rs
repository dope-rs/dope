use std::mem;

use crate::{
    process::{self, failure::bus, runtime::bus::Tx},
    shutdown,
};

/// Reports worker failure or consumes its shutdown result.
pub struct Terminal<'run> {
    pub(in crate::process) source: Tx<'run>,
}

impl<'run> Terminal<'run> {
    pub fn fail(&self) -> Result<bus::Event, bus::Fault> {
        self.source.send(bus::Event::Failed)?;
        Ok(bus::Event::Failed)
    }

    pub fn finish(
        self,
        result: shutdown::Requested<process::Shutdown<'run>>,
    ) -> Result<bus::Event, bus::Fault> {
        self.source.terminal(result)
    }
}

const _: () = {
    assert!(mem::size_of::<shutdown::Requested<process::Shutdown<'static>>>() == 0);
    assert!(mem::size_of::<Terminal<'static>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Terminal<'static>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Option<Terminal<'static>>>() == mem::size_of::<usize>());
};

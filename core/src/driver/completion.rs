use std::io;
use std::time::Duration;

use super::DriverContext;
use crate::backend::Backend;
use crate::backend::ops::completion::CompletionBackend;
use crate::io::Event;

pub trait Completion<'d> {
    fn drain(&mut self, buf: &mut [Option<Event<'d>>]) -> usize;
    fn wait(&mut self, timeout: Option<Duration>) -> io::Result<()>;
}

impl<'d> Completion<'d> for DriverContext<'_, 'd> {
    fn drain(&mut self, buf: &mut [Option<Event<'d>>]) -> usize {
        self.flush_returned_buffers();
        let reference = self.driver_ref();
        <Backend as CompletionBackend>::drain(self.backend(), reference, buf)
    }

    fn wait(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.flush_returned_buffers();
        <Backend as CompletionBackend>::wait(self.backend(), timeout)
    }
}

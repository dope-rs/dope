use std::{cell::Cell, num::NonZero};

use dope_core::{
    dope_sys::io::socket::record::{Fault, Record},
    platform::{channel, thread},
};

pub const STACK: NonZero<usize> = NonZero::<usize>::MIN.saturating_add(8 * 1024 * 1024 - 1);

pub struct Write<'a> {
    pub target: &'a mut u64,
    pub value: u64,
}

impl thread::Task for Write<'_> {
    type Output = u64;

    fn run(self) -> u64 {
        *self.target = self.value;
        self.value
    }
}

pub struct Gated<'a> {
    pub channel: channel::Channel,
    pub task: Write<'a>,
}

#[derive(Debug)]
pub enum Error {
    Send(Fault),
    Receive(Record),
}

impl thread::Task for Gated<'_> {
    type Output = Result<u64, Error>;

    fn run(self) -> Self::Output {
        self.channel.send(1).map_err(Error::Send)?;
        match self.channel.recv() {
            Record::Value(2) => Ok(thread::Task::run(self.task)),
            record => Err(Error::Receive(record)),
        }
    }
}

pub struct Owned<'a>(pub &'a mut u32);

impl thread::Task for Owned<'_> {
    type Output = Self;

    fn run(self) -> Self {
        self
    }
}

impl Drop for Owned<'_> {
    fn drop(&mut self) {
        *self.0 += 1;
    }
}

pub struct Order<'a> {
    pub order: &'a Cell<u32>,
    pub value: u32,
}

impl Drop for Order<'_> {
    fn drop(&mut self) {
        self.order.set(self.order.get() * 10 + self.value);
    }
}

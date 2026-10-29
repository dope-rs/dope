use dope_sys::{Errno, io::reactor};

#[derive(Debug)]
pub enum Acquire {
    Kqueue(reactor::Create),
    Register(Errno),
    Ignore(Errno),
}

use dope_sys::thread::signal::AcquireFault;

impl From<Acquire> for AcquireFault {
    fn from(error: Acquire) -> Self {
        match error {
            Acquire::Kqueue(error) => match error {
                reactor::Create::Open(error) => Self::QueueOpen(error),
                reactor::Create::Cloexec(error) => Self::QueueCloexec(error),
            },
            Acquire::Register(error) => Self::Register(error),
            Acquire::Ignore(error) => Self::Ignore(error),
        }
    }
}

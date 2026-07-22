use std::io;

use crate::DriverContext;

pub(in crate::runtime) struct SignalState;

impl SignalState {
    pub(in crate::runtime) fn new() -> io::Result<Self> {
        Err(unsupported())
    }

    pub(in crate::runtime) fn try_register(
        &self,
        _driver: &mut DriverContext<'_, '_>,
    ) -> io::Result<()> {
        Err(unsupported())
    }
}

fn unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "signal shutdown is only available on Linux",
    )
}

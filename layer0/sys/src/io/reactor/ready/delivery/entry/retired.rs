use crate::{
    Errno,
    io::{
        reactor::ready::delivery::entry::{Bytes, Context, Flow, Key, Unit},
        socket,
    },
};

/// The exact result of an operation whose original delivery was canceled.
pub enum Retired<C: Context> {
    Accept {
        key: Key<C>,
        result: Result<C::Reclaimed, C::Failure>,
        flow: Flow<C>,
    },
    RecvData {
        key: Key<C>,
        len: usize,
        flow: Flow<C>,
    },
    RecvControl {
        key: Key<C>,
        result: Unit<C>,
        flow: Flow<C>,
    },
    Send {
        key: Key<C>,
        result: Bytes<C>,
    },
    Connect {
        key: Key<C>,
        result: Unit<C>,
    },
    Create {
        key: Key<C>,
        slot: C::Outbound,
        result: Result<(), socket::Create>,
        close: Option<Result<(), Errno>>,
    },
    Open {
        key: Key<C>,
        result: Result<(), Errno>,
        close: Option<Result<(), Errno>>,
    },
    Write {
        key: Key<C>,
        result: Bytes<C>,
    },
    Sync {
        key: Key<C>,
        result: Unit<C>,
    },
    Shutdown,
}

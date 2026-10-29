/// One validated scalar stream option in its fixed application order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Kind {
    NoDelay,
    KeepAlive,
    RecvBuffer,
    SendBuffer,
    KeepAliveIdle,
    KeepAliveInterval,
    KeepAliveRetries,
}

impl Kind {
    pub const fn bit(self) -> u8 {
        1 << self as u8
    }
}

pub mod bounded;
pub mod redial;
pub mod yielding;

#[derive(Debug)]
pub enum Error {
    Received,
    Connected(bounded::Phase),
    Active(bounded::Phase),
    Closing(bounded::Phase),
    Drained(bounded::Phase),
    Bytes(Vec<u8>),
    BoundedShutdown { left: usize, done: u32 },
    PrepareConnections(u32),
    CloseConnections(u32),
    YieldingShutdown { closed: u32, retired: u32 },
    Flush { pending: bool, empty: bool },
    Closed { conns: u32, closed: u32 },
}

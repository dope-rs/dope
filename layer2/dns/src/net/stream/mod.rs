pub(crate) mod codec;
mod cursor;
mod link;
mod logic;
mod peers;

pub(crate) use logic::Logic;
pub(crate) use peers::Set;

pub(crate) const CONNECTIONS: usize = 1;
pub(crate) const TRANSACTION_IDS: u32 = 1 << 16;

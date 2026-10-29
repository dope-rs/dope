#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(in crate::driver::schedule::ready) enum Kind {
    Free,
    Reserved,
    Dispatch,
    Task,
    Topology,
    Retired,
}

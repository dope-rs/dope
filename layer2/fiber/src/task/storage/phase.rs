use core::mem;

/// The exact data-bearing phase of one current task-set entry.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum State {
    Admitting,
    Running,
    Queued,
    Completed,
    External,
}

const _: () = assert!(mem::size_of::<State>() == 1);

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub(in crate::task::storage) enum State {
    Free,
    Retired,
    Admitting,
    Running,
    Queued,
    Completed,
    External,
    Retiring,
}

impl State {
    pub(in crate::task::storage) const fn is_occupied(self) -> bool {
        matches!(
            self,
            Self::Admitting | Self::Running | Self::Queued | Self::Completed | Self::External
        )
    }
}

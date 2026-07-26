#[derive(Clone, Copy, Debug)]
pub(crate) struct BufferId(u16);

impl BufferId {
    /// # Safety
    /// `raw` must identify one live provided buffer uniquely owned by the
    /// caller.
    pub(crate) const unsafe fn new(raw: u16) -> Self {
        Self(raw)
    }

    pub(crate) const fn into_raw(self) -> u16 {
        self.0
    }
}

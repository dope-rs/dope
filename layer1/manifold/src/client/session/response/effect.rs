use crate::client::session::response::flow;

pub enum Effect {
    Committed,
    Close,
    RetryInput,
    Yield,
    Blocked,
}

impl Effect {
    pub(super) const fn from_end(end: flow::End) -> Self {
        match end {
            flow::End::Continue => Self::Committed,
            flow::End::Close => Self::Close,
        }
    }
}

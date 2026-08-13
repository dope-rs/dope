use dope_core::io::event::open;

pub(super) enum Opening {
    Opened(open::Opened),
    OpenFailed(i32),
    Stat(crate::StatEvent),
}

impl Opening {
    pub(super) fn from_open(outcome: open::Outcome) -> Self {
        match outcome {
            open::Outcome::Opened(opened) => Self::Opened(opened),
            open::Outcome::Failed(errno) => Self::OpenFailed(errno),
        }
    }
}

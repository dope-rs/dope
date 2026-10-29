pub(in crate::network::listener) use schedule::Schedule;

pub(in crate::network::listener) mod accept;
pub(super) mod control;
pub(super) mod install;
mod policy;
pub(super) mod prepared;
mod schedule;

#[derive(Clone, Copy)]
pub(in crate::network::listener) enum Phase {
    Accept,
    Ingress,
    Credit,
    Dirty,
    Deadline,
}

impl Phase {
    pub(in crate::network::listener) const COUNT: usize = 5;

    pub(in crate::network::listener) const fn next(self) -> Self {
        match self {
            Self::Accept => Self::Ingress,
            Self::Ingress => Self::Credit,
            Self::Credit => Self::Dirty,
            Self::Dirty => Self::Deadline,
            Self::Deadline => Self::Accept,
        }
    }
}

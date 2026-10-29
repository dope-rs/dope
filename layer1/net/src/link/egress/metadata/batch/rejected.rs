use crate::link::egress::{self, load::Value};

/// A batch rejection retaining the payload's checked geometry for retry.
#[must_use]
#[derive(Debug)]
pub struct Rejected<T> {
    pub load: egress::NonEmpty<T>,
    pub reason: egress::Rejection,
}

const _: () = assert!(
    std::mem::size_of::<Rejected<()>>() + 2 * std::mem::size_of::<usize>()
        == std::mem::size_of::<egress::Rejected<Value<()>>>()
);

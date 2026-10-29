use crate::task::storage::fixed::schedule::queue::{Flat, Owned};

pub trait Proof {}

impl Proof for Flat<'_> {}

impl<S> Proof for Owned<'_, S> {}

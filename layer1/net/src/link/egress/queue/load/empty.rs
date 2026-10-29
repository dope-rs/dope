use o3::buffer::storage;

use crate::link::egress::{Load, Rejected, load};

/// An owned load paired with the proof that it has no logical bytes.
#[must_use]
pub struct Empty<B> {
    load: Load<B>,
    proof: storage::Zero,
}

impl<B> Empty<B> {
    pub fn into_parts(self) -> (Load<B>, storage::Zero) {
        (self.load, self.proof)
    }
}

impl<B> TryFrom<Rejected<Load<B>, load::Error>> for Empty<B> {
    type Error = Rejected<Load<B>, load::Error>;

    fn try_from(rejected: Rejected<Load<B>, load::Error>) -> Result<Self, Self::Error> {
        let (load, reason) = rejected.into_parts();
        match reason {
            load::Error::Empty(proof) => Ok(Self { load, proof }),
            reason => Err(Rejected::new(load, reason)),
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Empty<()>>() == std::mem::size_of::<Load<()>>());
    assert!(std::mem::align_of::<Empty<()>>() == std::mem::align_of::<Load<()>>());
};

use dope_quic::stream::send::{self, admit};
use dope_quic::wire::stream::Id;
use o3::buffer::storage;

pub struct Retain;

pub struct Observe<F>(pub F);

impl<W, V, F: FnMut(&W) -> V> admit::Commit<storage::Shared, W> for Observe<F> {
    type Observation = V;
    type Retained = V;
    type Discarded = (V, admit::Discard<W>);

    fn observe(&mut self, owner: &W) -> V {
        (self.0)(owner)
    }

    fn retained(self, observation: V, _: admit::Admission<'_, storage::Shared, W>) -> V {
        observation
    }

    fn discarded(self, observation: V, cause: admit::Discard<W>) -> Self::Discarded {
        (observation, cause)
    }
}

impl<W> admit::Commit<storage::Shared, W> for Retain {
    type Observation = ();
    type Retained = (Id, send::Boundary, Vec<*const u8>);
    type Discarded = admit::Discard<W>;

    fn observe(&mut self, _: &W) {}

    fn retained(self, (): (), admit: admit::Admission<'_, storage::Shared, W>) -> Self::Retained {
        (
            admit.source().id(),
            admit.boundary(),
            admit
                .retained()
                .map(|owner| owner.as_slice().as_ptr())
                .collect(),
        )
    }

    fn discarded(self, (): (), cause: admit::Discard<W>) -> Self::Discarded {
        cause
    }
}

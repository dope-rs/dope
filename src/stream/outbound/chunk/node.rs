use o3::buffer::{self, hold, segments};

use crate::stream::{outbound, release, send};

/// One send-queue node retaining contiguous storage or a segment chain.
pub(crate) enum Node<O: buffer::raw::Segment> {
    Buffer(send::Buffer<O>),
    One(segments::Single<O>),
    Segments(segments::Many<O>),
}

impl<O: buffer::raw::Segment> Node<O> {
    pub(in crate::stream) fn charge(&self) -> hold::Charge {
        match self {
            Self::Buffer(send::Buffer::Inline(_)) => hold::Charge::default(),
            Self::Buffer(buffer) => hold::Charge::from_owner(buffer.len(), buffer.resident()),
            Self::One(owner) => {
                hold::Charge::from_owner(owner.len().get(), buffer::Residency::resident(owner))
            }
            Self::Segments(owners) => hold::Charge::from(owners.as_chain()),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Buffer(buffer) => buffer.len(),
            Self::One(owner) => owner.len().get(),
            Self::Segments(segments) => segments.len().get(),
        }
    }

    pub(in crate::stream::outbound) fn resident(&self) -> usize {
        match self {
            Self::Buffer(buffer) => buffer.resident(),
            Self::One(owner) => buffer::Residency::resident(owner),
            Self::Segments(segments) => buffer::Residency::resident(segments.as_chain()),
        }
    }

    pub(in crate::stream::outbound) fn owners(&self) -> usize {
        match self {
            Self::Buffer(buffer) => usize::from(!buffer.is_empty()),
            Self::One(_) => 1,
            Self::Segments(segments) => segments.as_chain().count().into_raw() as usize,
        }
    }

    pub(in crate::stream) fn release<M: release::Mode>(
        self,
        buffers: &mut outbound::BufferCache,
    ) -> (outbound::Charge, M::Projection) {
        let proof = M::project(|| self.charge());
        let charge = outbound::Charge {
            resident_bytes: self.resident(),
            segments: self.owners(),
            ..outbound::Charge::default()
        };
        self.recycle_into(buffers);
        (charge, proof)
    }

    fn recycle_into(self, buffers: &mut outbound::BufferCache) {
        if let Self::Buffer(buffer) = self {
            buffer.recycle_into(buffers);
        }
    }
}

const _: () = {
    use o3::buffer::{resident, storage};
    assert!(size_of::<Node<storage::Shared>>() == 4 * size_of::<usize>());
    assert!(size_of::<Option<Node<storage::Shared>>>() == size_of::<Node<storage::Shared>>());
    assert!(
        size_of::<Option<Node<resident::Shared<'static>>>>()
            == size_of::<Node<resident::Shared<'static>>>()
    );
};

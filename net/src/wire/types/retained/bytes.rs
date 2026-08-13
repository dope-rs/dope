use std::{fmt, ops};

use dope_core::io::recv;
use o3::buffer::{self, bytes, resident, storage};

use crate::wire;

enum Storage<'d> {
    Provided(recv::Retained<'d>),
    Buffered(bytes::Bytes<bytes::Retained>),
    Pooled(bytes::Bytes<bytes::Pooled<'d>>),
}

pub struct RetainedBytes<'d> {
    storage: Storage<'d>,
    credit: Option<wire::ErasedRecvCreditGuard<'d>>,
}

impl<'d> RetainedBytes<'d> {
    pub(crate) fn copy_from_slice(
        budget: &resident::Budget<'d>,
        bytes: &[u8],
    ) -> Result<Self, buffer::CapacityError> {
        let mut snapshot =
            resident::Snapshot::<{ u32::MAX as usize }>::with_capacity_up_to(budget, bytes.len())?;
        snapshot.try_extend(bytes)?;
        Ok(Self::from(snapshot.snapshot().unwrap_or_default()))
    }

    pub fn from_provided(bytes: recv::Retained<'d>) -> Self {
        Self {
            storage: Storage::Provided(bytes),
            credit: None,
        }
    }

    pub fn from_buffered(bytes: bytes::Bytes<bytes::Retained>) -> Self {
        Self {
            storage: Storage::Buffered(bytes),
            credit: None,
        }
    }

    pub fn from_pooled(bytes: bytes::Bytes<bytes::Pooled<'d>>) -> Self {
        Self {
            storage: Storage::Pooled(bytes),
            credit: None,
        }
    }

    pub fn with_credit(mut self, credit: wire::ErasedRecvCreditGuard<'d>) -> Self {
        self.credit = Some(credit);
        self
    }

    pub fn as_slice(&self) -> &[u8] {
        match &self.storage {
            Storage::Provided(bytes) => bytes.as_slice(),
            Storage::Buffered(bytes) => bytes.as_slice(),
            Storage::Pooled(bytes) => bytes.as_slice(),
        }
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    pub fn resident_bytes(&self) -> usize {
        match &self.storage {
            Storage::Provided(bytes) => bytes.resident_bytes(),
            Storage::Buffered(bytes) => bytes.resident_bytes(),
            Storage::Pooled(bytes) => bytes.resident_bytes(),
        }
    }

    pub fn get(&self, range: ops::Range<usize>) -> Option<Self> {
        let storage = match &self.storage {
            Storage::Provided(bytes) => Storage::Provided(bytes.get(range)?),
            Storage::Buffered(bytes) => Storage::Buffered(bytes.clone().get(range)?),
            Storage::Pooled(bytes) => Storage::Pooled(bytes.clone().get(range)?),
        };
        Some(Self {
            storage,
            credit: self.credit.clone(),
        })
    }

    /// Narrows this owner in place without retaining another receive owner.
    pub fn into_range(self, range: ops::Range<usize>) -> Option<Self> {
        let Self { storage, credit } = self;
        let storage = match storage {
            Storage::Provided(bytes) => Storage::Provided(bytes.into_range(range).ok()?),
            Storage::Buffered(bytes) => Storage::Buffered(bytes.get(range)?),
            Storage::Pooled(bytes) => Storage::Pooled(bytes.get(range)?),
        };
        Some(Self { storage, credit })
    }

    pub fn try_advance(&mut self, amount: usize) -> bool {
        if amount > self.len() {
            return false;
        }
        match &mut self.storage {
            Storage::Provided(bytes) => bytes.advance(amount),
            Storage::Buffered(bytes) => {
                if !bytes.try_advance(amount) {
                    return false;
                }
            }
            Storage::Pooled(bytes) => {
                if !bytes.try_advance(amount) {
                    return false;
                }
            }
        }
        true
    }

    pub fn into_shared(self) -> storage::Shared {
        match self.storage {
            Storage::Provided(bytes) => storage::Shared::copy_from_slice(bytes.as_slice()),
            Storage::Buffered(bytes) => bytes.into_shared(),
            Storage::Pooled(bytes) => bytes.into_shared(),
        }
    }
}

impl AsRef<[u8]> for RetainedBytes<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl buffer::PrefixLength for RetainedBytes<'_> {
    fn prefix_len(&self) -> usize {
        self.len()
    }
}

impl buffer::PrefixConsumer for RetainedBytes<'_> {
    fn consume_validated_prefix(&mut self, proof: buffer::PrefixProof) {
        let advanced = self.try_advance(proof.amount());
        debug_assert!(advanced);
    }
}

impl ops::Deref for RetainedBytes<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl fmt::Debug for RetainedBytes<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("RetainedBytes")
            .field(&self.as_slice())
            .finish()
    }
}

impl PartialEq for RetainedBytes<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for RetainedBytes<'_> {}

impl Clone for RetainedBytes<'_> {
    fn clone(&self) -> Self {
        let storage = match &self.storage {
            Storage::Provided(bytes) => Storage::Provided(bytes.clone()),
            Storage::Buffered(bytes) => Storage::Buffered(bytes.clone()),
            Storage::Pooled(bytes) => Storage::Pooled(bytes.clone()),
        };
        Self {
            storage,
            credit: self.credit.clone(),
        }
    }
}

impl From<storage::Shared> for RetainedBytes<'_> {
    fn from(bytes: storage::Shared) -> Self {
        Self::from_buffered(bytes::Retainable::into_retained(bytes))
    }
}

impl From<bytes::Bytes<bytes::Retained>> for RetainedBytes<'_> {
    fn from(bytes: bytes::Bytes<bytes::Retained>) -> Self {
        Self::from_buffered(bytes)
    }
}

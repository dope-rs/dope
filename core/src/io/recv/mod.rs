use std::ops;

use o3::buffer::resident;

use crate::{driver, io};

pub mod raw;
mod sealed;

pub(in crate::io::recv) use sealed::{Owner, Proof};

pub(crate) struct Span {
    offset: usize,
    len: usize,
}

pub struct Lease<'d> {
    owner: Owner<'d>,
    region: raw::Region,
}

pub struct View<'d> {
    owner: Owner<'d>,
    region: raw::Region,
}

pub struct Unique<'d> {
    driver: driver::Reference<'d>,
    owner: driver::RecvOwner<'d>,
    region: raw::Region,
}

pub struct Shared<'d> {
    driver: driver::Reference<'d>,
    owner: driver::RecvOwner<'d>,
    region: raw::Region,
}

pub struct Retained<'d> {
    driver: driver::Reference<'d>,
    owner: driver::AccountedRecvOwner<'d>,
    region: raw::Region,
}

const _: () = {
    assert!(std::mem::size_of::<Shared<'static>>() == 4 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Retained<'static>>() == 4 * std::mem::size_of::<usize>());
};

impl<'d> Lease<'d> {
    pub(crate) fn from_completion(
        driver: driver::Reference<'d>,
        buffer: io::Buffer,
        region: raw::Region,
    ) -> Self {
        Self {
            owner: Owner::new(driver, buffer),
            region,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.region.as_slice()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.region.as_mut_slice()
    }

    pub fn advance(&mut self, count: usize) {
        assert!(
            self.region.advance(count),
            "dope: recv lease advance out of bounds"
        );
    }

    pub(crate) fn span_of(&self, bytes: &[u8]) -> Span {
        let base = self.region.ptr.as_ptr().addr();
        let start = bytes.as_ptr().addr();
        Span {
            offset: start.wrapping_sub(base),
            len: bytes.len(),
        }
    }

    pub fn into_view(self) -> View<'d> {
        let Self { owner, region } = self;
        View { owner, region }
    }

    pub(crate) fn into_subview(self, span: Span) -> Result<View<'d>, Self> {
        let Self {
            owner,
            region: source,
        } = self;
        let Some(region) = source.subregion(span.offset, span.len) else {
            return Err(Self {
                owner,
                region: source,
            });
        };
        Ok(View { owner, region })
    }

    pub fn into_shared(self) -> Shared<'d> {
        let Self { owner, region } = self;
        let (driver, owner) = owner.share();
        Shared {
            driver,
            owner,
            region,
        }
    }
}

impl AsRef<[u8]> for Lease<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<'d> View<'d> {
    pub fn as_slice(&self) -> &[u8] {
        self.region.as_slice()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.region.as_mut_slice()
    }

    pub fn len(&self) -> usize {
        self.region.len()
    }

    pub fn is_empty(&self) -> bool {
        self.region.len() == 0
    }

    pub fn advance(&mut self, count: usize) {
        assert!(
            self.region.advance(count),
            "dope: recv view advance out of bounds"
        );
    }

    pub fn into_shared(self) -> Shared<'d> {
        let Self { owner, region } = self;
        let (driver, owner) = owner.share();
        Shared {
            driver,
            owner,
            region,
        }
    }

    pub fn into_unique(self) -> Unique<'d> {
        let Self { owner, region } = self;
        let (driver, owner) = owner.share();
        Unique {
            driver,
            owner,
            region,
        }
    }

    pub fn try_into_retained(self, budget: &resident::Budget<'d>) -> Result<Retained<'d>, Self> {
        let Self { owner, region } = self;
        match owner.retain(budget) {
            Ok((driver, owner)) => Ok(Retained {
                driver,
                owner,
                region,
            }),
            Err(owner) => Err(Self { owner, region }),
        }
    }
}

impl AsRef<[u8]> for View<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<'d> Unique<'d> {
    pub fn as_slice(&self) -> &[u8] {
        self.region.as_slice()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.region.as_mut_slice()
    }

    pub fn len(&self) -> usize {
        self.region.len()
    }

    pub fn is_empty(&self) -> bool {
        self.region.len() == 0
    }

    pub fn split_at(mut self, mid: usize) -> Result<(Self, Self), Self> {
        let Some(region) = self.region.split_off(mid) else {
            return Err(self);
        };
        let owner = self.driver.receive().retain_recv_owner(&self.owner);
        let tail = Self {
            driver: self.driver,
            owner,
            region,
        };
        Ok((self, tail))
    }

    pub fn into_shared(self) -> Shared<'d> {
        let (driver, owner, region) = <Self as Proof<'d>>::into_parts(self);
        Shared {
            driver,
            owner,
            region,
        }
    }
}

impl Drop for Unique<'_> {
    fn drop(&mut self) {
        self.driver.receive().release_recv_owner(&self.owner);
    }
}

impl AsRef<[u8]> for Unique<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsMut<[u8]> for Unique<'_> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl<'d> Shared<'d> {
    pub fn as_slice(&self) -> &[u8] {
        self.region.as_slice()
    }

    pub fn len(&self) -> usize {
        self.region.len()
    }

    pub fn is_empty(&self) -> bool {
        self.region.len() == 0
    }

    pub fn resident_bytes(&self) -> usize {
        self.driver.receive().layout().buffer_len()
    }

    pub fn get(&self, range: ops::Range<usize>) -> Option<Self> {
        let region = self.region.subregion(range.start, range.len())?;
        let owner = self.driver.receive().retain_recv_owner(&self.owner);
        Some(Self {
            driver: self.driver,
            owner,
            region,
        })
    }

    pub fn accounted(
        &self,
        range: ops::Range<usize>,
        budget: &resident::Budget<'d>,
    ) -> Option<Retained<'d>> {
        let region = self.region.subregion(range.start, range.len())?;
        let owner = self
            .driver
            .receive()
            .retain_accounted_recv_owner(&self.owner, budget)?;
        Some(Retained {
            driver: self.driver,
            owner,
            region,
        })
    }

    pub fn advance(&mut self, count: usize) {
        assert!(
            self.region.advance(count),
            "dope: shared recv advance out of bounds"
        );
    }
}

impl Drop for Shared<'_> {
    fn drop(&mut self) {
        self.driver.receive().release_recv_owner(&self.owner);
    }
}

impl AsRef<[u8]> for Shared<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<'d> Retained<'d> {
    pub fn as_slice(&self) -> &[u8] {
        self.region.as_slice()
    }

    pub fn len(&self) -> usize {
        self.region.len()
    }

    pub fn is_empty(&self) -> bool {
        self.region.len() == 0
    }

    pub fn resident_bytes(&self) -> usize {
        self.driver.receive().layout().buffer_len()
    }

    pub fn get(&self, range: ops::Range<usize>) -> Option<Self> {
        let region = self.region.subregion(range.start, range.len())?;
        let owner = self
            .driver
            .receive()
            .retain_existing_accounted_recv_owner(&self.owner);
        Some(Self {
            driver: self.driver,
            owner,
            region,
        })
    }

    pub fn into_range(mut self, range: ops::Range<usize>) -> Result<Self, Self> {
        let Some(len) = range.end.checked_sub(range.start) else {
            return Err(self);
        };
        let Some(region) = self.region.subregion(range.start, len) else {
            return Err(self);
        };
        self.region = region;
        Ok(self)
    }

    pub(crate) fn duplicate(&self) -> Self {
        let owner = self
            .driver
            .receive()
            .retain_existing_accounted_recv_owner(&self.owner);
        Self {
            driver: self.driver,
            owner,
            region: self.region.duplicate(),
        }
    }

    pub fn advance(&mut self, count: usize) {
        assert!(
            self.region.advance(count),
            "dope: retained recv advance out of bounds"
        );
    }
}

impl Drop for Retained<'_> {
    fn drop(&mut self) {
        self.driver
            .receive()
            .release_accounted_recv_owner(&self.owner);
    }
}

impl Clone for Retained<'_> {
    fn clone(&self) -> Self {
        self.duplicate()
    }
}

impl AsRef<[u8]> for Retained<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsMut<[u8]> for View<'_> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

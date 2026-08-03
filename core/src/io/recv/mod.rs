use o3::permit::{ReturnPermit, ReturnTo};

use self::completion::Completion;
use self::raw::Region;
use crate::backend::RecvBuffer;
use crate::driver::{DriverContext, DriverRef};

pub(crate) mod completion;
pub(crate) mod raw;

pub struct Span {
    offset: usize,
    len: usize,
}

pub struct Lease<'d> {
    permit: ReturnPermit<Return<'d>>,
    region: Region,
}

pub struct View<'d> {
    _permit: ReturnPermit<Return<'d>>,
    region: Region,
}

struct Return<'d>(DriverRef<'d>);

impl ReturnTo for Return<'_> {
    type Item = RecvBuffer;

    fn return_item(&self, buffer: RecvBuffer) {
        self.0.return_buffer(buffer);
    }
}

impl<'d> Lease<'d> {
    pub(crate) fn from_completion(driver: DriverRef<'d>, completion: Completion) -> Self {
        let Completion { buffer, region } = completion;
        Self {
            permit: ReturnPermit::new(Return(driver), buffer),
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

    pub fn range_of(&self, bytes: &[u8]) -> Option<Span> {
        let base = self.region.ptr.as_ptr().addr();
        let start = bytes.as_ptr().addr();
        let offset = start.checked_sub(base)?;
        self.span(offset, bytes.len())
    }

    pub fn span(&self, offset: usize, len: usize) -> Option<Span> {
        if offset > self.region.len || len > self.region.len - offset {
            return None;
        }
        Some(Span { offset, len })
    }

    pub fn into_view(self, span: Span) -> Result<View<'d>, Self> {
        let Self {
            permit,
            region: source,
        } = self;
        let Some(region) = source.subregion(span.offset, span.len) else {
            return Err(Self {
                permit,
                region: source,
            });
        };
        Ok(View {
            _permit: permit,
            region,
        })
    }

    pub fn release(self, driver: &mut DriverContext<'_, 'd>) {
        driver.release_buffer(self.permit.into_item());
    }
}

impl AsRef<[u8]> for Lease<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl View<'_> {
    pub fn as_slice(&self) -> &[u8] {
        self.region.as_slice()
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
}

impl AsRef<[u8]> for View<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

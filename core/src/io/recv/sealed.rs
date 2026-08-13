use std::{mem, ptr};

use o3::{buffer::resident, permit};

use crate::{driver, io};

pub(in crate::io::recv) struct Owner<'d> {
    lease: permit::Lease<ReturnBuffer<'d>>,
}

struct ReturnBuffer<'d> {
    driver: driver::Reference<'d>,
}

impl permit::Return for ReturnBuffer<'_> {
    type Item = io::Buffer;

    fn return_item(&self, buffer: Self::Item) {
        self.driver.maintenance().return_buffer(buffer);
    }
}

pub(in crate::io::recv) trait Proof<'d> {
    fn into_parts(
        self,
    ) -> (
        driver::Reference<'d>,
        driver::RecvOwner<'d>,
        super::raw::Region,
    );
}

impl<'d> Owner<'d> {
    pub(in crate::io::recv) fn new(driver: driver::Reference<'d>, buffer: io::Buffer) -> Self {
        Self {
            lease: permit::Lease::new(ReturnBuffer { driver }, buffer),
        }
    }

    fn into_parts(self) -> (driver::Reference<'d>, io::Buffer) {
        let (return_buffer, buffer) = self.lease.into_parts();
        (return_buffer.driver, buffer)
    }

    pub(in crate::io::recv) fn share(self) -> (driver::Reference<'d>, driver::RecvOwner<'d>) {
        let (driver, buffer) = self.into_parts();
        let owner = driver.receive().retain_buffer(buffer);
        (driver, owner)
    }

    pub(in crate::io::recv) fn retain(
        self,
        budget: &resident::Budget<'d>,
    ) -> Result<(driver::Reference<'d>, driver::AccountedRecvOwner<'d>), Self> {
        let (driver, buffer) = self.into_parts();
        match driver.receive().retain_accounted_buffer(buffer, budget) {
            Ok(owner) => Ok((driver, owner)),
            Err(buffer) => Err(Self::new(driver, buffer)),
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<Owner<'static>>()
            == mem::size_of::<(driver::Reference<'static>, io::Buffer)>()
    );
    assert!(
        mem::align_of::<Owner<'static>>()
            == mem::align_of::<(driver::Reference<'static>, io::Buffer)>()
    );
};

impl<'d> Proof<'d> for super::Unique<'d> {
    fn into_parts(
        self,
    ) -> (
        driver::Reference<'d>,
        driver::RecvOwner<'d>,
        super::raw::Region,
    ) {
        let this = mem::ManuallyDrop::new(self);
        let owner = unsafe { ptr::read(&this.owner) };
        let region = unsafe { ptr::read(&this.region) };
        (this.driver, owner, region)
    }
}

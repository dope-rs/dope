use std::marker;

use dope_core::driver;
use o3::buffer::write::{self, Sink as _};

use crate::file::appender;

#[must_use = "an append record must be committed or its staged bytes are discarded"]
pub struct Record<'a, 'd> {
    pub(super) bytes: write::Staged<'a>,
    pub(super) driver: marker::PhantomData<driver::Reference<'d>>,
}

impl Record<'_, '_> {
    pub fn try_extend(&mut self, bytes: &[u8]) -> Result<(), appender::Capacity> {
        let current = self.bytes.len();
        self.bytes.write_slice(bytes).map_err(|cause| {
            let (required, spare) = cause.into_parts();
            appender::Capacity {
                current,
                added: required.get(),
                maximum: current + spare,
            }
        })
    }

    pub fn commit(self) {
        self.bytes.commit();
    }
}

const _: () = {
    assert!(size_of::<Record<'static, 'static>>() == 2 * size_of::<usize>());
    assert!(size_of::<Option<Record<'static, 'static>>>() == size_of::<Record<'static, 'static>>());
};

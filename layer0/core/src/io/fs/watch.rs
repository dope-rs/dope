use std::mem::MaybeUninit;

use dope_sys::io::file::File;
use o3::buffer::bytes::bounded;

pub use crate::watch::{Event, Watch};
pub mod raw;

use crate::{Backend, driver::route, io::fs};

impl<'a, 'd, Tag: route::Tag> fs::Submission<'a, 'd, Tag, route::kind::Read> {
    pub fn watch(
        watch: &'a Watch,
        output: bounded::Value<&'a mut [MaybeUninit<u8>]>,
        target: route::Target<'d, Tag>,
    ) -> Self
    where
        Tag: route::Select<route::kind::Read>,
    {
        Self::bind(
            <Backend as File>::Request::watch(watch, output),
            target.operation::<route::kind::Read>(),
        )
    }
}

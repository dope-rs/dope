use dope_core::driver::route::KeyTag;

use crate::service::bindings::Bound;

const _: () = assert!(
    std::mem::size_of::<Bound<'static, 'static, u64, (), KeyTag<0>, 1>>()
        == std::mem::size_of::<&'static u64>()
);
const _: () = assert!(
    std::mem::size_of::<Bound<'static, 'static, u64, usize, KeyTag<0>, 1>>()
        == std::mem::size_of::<(&'static u64, usize)>()
);

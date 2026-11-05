pub mod common;

use dope_core::{
    driver::{
        life::finish,
        route::KeyTag,
        schedule::ready::{self, task, wake::store},
    },
    io::{
        datagram,
        fd::{handles, outbound},
    },
};

use crate::common::driver::task::Index;

#[test]
fn keeps_local_layouts() {
    let layout = if datagram::GSO_LIMITS.is_some() {
        (24, 8)
    } else {
        (0, 1)
    };
    assert_eq!(size_of::<datagram::GsoControl>(), layout.0);
    assert_eq!(align_of::<datagram::GsoControl>(), layout.1);
    assert_eq!(size_of::<Option<datagram::GsoControl>>(), layout.0);
    assert_eq!(
        size_of::<handles::Descriptor<'static>>(),
        2 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<ready::Slot<'static, KeyTag<1>>>(),
        2 * size_of::<u64>()
    );
    assert_eq!(
        size_of::<task::raw::Node<'static, Index>>(),
        3 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<task::raw::Binding<'static, 'static, Index>>(),
        4 * size_of::<usize>()
    );
    assert_eq!(size_of::<store::Entry<'static>>(), size_of::<usize>());
    assert_eq!(
        size_of::<finish::Finish<'static, 'static>>(),
        2 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<outbound::Reserved<'static, KeyTag<0>>>(),
        size_of::<usize>()
    );
}

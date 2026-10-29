use std::{marker::PhantomPinned, mem, pin, pin::Pin, ptr};

use dope_core::driver::route::KeyTag;
use dope_manifold::service::bindings;

#[test]
fn preserves_root_on_move() {
    let root = [7, 11];
    let mut bound = bindings::Bound::<_, _, KeyTag<0>, 1>::bind(&root, ());
    assert_eq!(mem::size_of_val(&bound), mem::size_of::<usize>());
    let pinned = Pin::new(&mut bound);
    assert!(ptr::eq(pinned.root(), &root));
    let moved = mem::replace(pinned.get_mut(), bindings::Bound::bind(&root, ()));
    assert!(ptr::eq(moved.root(), &root));
}

#[test]
fn pins_logic_without_unpin() {
    let root = [7, 11];
    let bound = pin::pin!(bindings::Bound::<_, _, KeyTag<0>, 1>::bind(
        &root,
        PhantomPinned
    ));
    assert_eq!(
        mem::size_of_val(bound.as_ref().get_ref()),
        mem::size_of::<usize>()
    );
    assert!(ptr::eq(bound.as_ref().root(), &root));
}

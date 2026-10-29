use std::{marker, num, ptr};

use crate::link::egress::load::credit::wait::tree;

pub(in crate::link::egress::load::credit::wait) struct Nodes<'a> {
    pub(super) pointer: ptr::NonNull<tree::Node>,
    pub(super) borrow: marker::PhantomData<&'a mut [tree::Node]>,
}

pub(in crate::link::egress::load::credit::wait) struct NodeView<'a> {
    pub(super) pointer: ptr::NonNull<tree::Node>,
    pub(super) len: num::NonZero<u32>,
    pub(super) borrow: marker::PhantomData<&'a [tree::Node]>,
}

impl Nodes<'_> {
    pub(in crate::link::egress::load::credit::wait) fn get(
        &self,
        index: tree::Index,
    ) -> &tree::Node {
        // SAFETY: indices enter this wait table through an admitted credit lane
        // and tree links preserve that origin; Core initializes the full plane.
        unsafe { &*self.pointer.as_ptr().add(index.offset()) }
    }

    pub(in crate::link::egress::load::credit::wait) fn get_mut(
        &mut self,
        index: tree::Index,
    ) -> &mut tree::Node {
        // SAFETY: the index belongs to this initialized plane; &mut self retains
        // its exclusive loan from Core::parts for the returned node borrow.
        unsafe { &mut *self.pointer.as_ptr().add(index.offset()) }
    }
}

impl NodeView<'_> {
    pub(in crate::link::egress::load::credit::wait) const fn len(&self) -> usize {
        self.len.get() as usize
    }

    pub(in crate::link::egress::load::credit::wait) fn get(
        &self,
        index: tree::Index,
    ) -> &tree::Node {
        // SAFETY: the selected index remains in this tree's initialized plane;
        // the view retains its shared Core borrow for the returned node borrow.
        unsafe { &*self.pointer.as_ptr().add(index.offset()) }
    }
}

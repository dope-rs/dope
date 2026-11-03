use o3::buffer;

use crate::io::recv;

// SAFETY: Lease delegates both views to its bounded provided Unique.
// AsMut returns the same stored slice without changing its bytes or descriptor.
unsafe impl buffer::raw::Mutable for recv::Lease<'_> {}

// SAFETY: the bounded provided owner admits and returns the exact disjoint
// prefix without changing its owner, initialized contents or driver lifetime.
unsafe impl buffer::raw::Split for recv::Lease<'_> {}

// SAFETY: the lease retains its driver and provided owner while the exact
// bounded Unique applies its existing restriction and empty-owner policy.
unsafe impl buffer::raw::Store for recv::Lease<'_> {
    fn empty() -> Option<Self> {
        None
    }

    fn restrict(slice: buffer::raw::Slice<'_, '_, Self>) {
        // SAFETY: the transparent lease exposes only its original bounded Unique.
        buffer::raw::Store::project(unsafe { slice.map(|lease| &mut lease.0) });
    }
}

// SAFETY: conversion moves the same bounded provided allocation, extent and owner.
unsafe impl<'d> buffer::raw::Freeze for recv::Lease<'d> {
    type Output = recv::Shared<'d>;

    fn freeze(self) -> Self::Output {
        self.into()
    }
}

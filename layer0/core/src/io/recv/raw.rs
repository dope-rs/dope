use o3::buffer;

use crate::io::recv;

// SAFETY: Lease delegates both views to its bounded provided Unique.
// AsMut returns the same stored slice without changing its bytes or descriptor.
unsafe impl buffer::raw::Mutable for recv::Lease<'_> {}

// SAFETY: the bounded provided owner admits and returns the exact disjoint
// prefix without changing its owner, initialized contents or driver lifetime.
unsafe impl buffer::raw::Split for recv::Lease<'_> {}

// SAFETY: conversion moves the same bounded provided allocation, extent and owner.
unsafe impl<'d> buffer::raw::Freeze for recv::Lease<'d> {
    type Output = recv::Shared<'d>;

    fn freeze(self) -> Self::Output {
        self.into()
    }
}

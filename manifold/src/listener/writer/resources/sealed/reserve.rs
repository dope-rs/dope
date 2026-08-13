use std::ptr;

use o3::collections::fixed::pinned::recycle;

use crate::listener::writer::resources;

// SAFETY: DirectSource is private to resources and is constructed only from a
// Retention issued by writer::Owner. Owner stores the lease-owning connection
// pool before the Arena, so every direct lease releases before this pool.
unsafe impl<'d, const ID: u8> recycle::raw::PoolOwner<'d, resources::Flight<'d, ID>>
    for resources::DirectSource<'d, ID>
{
    fn pool(self) -> ptr::NonNull<recycle::Pool<resources::Flight<'d, ID>>> {
        self.pool
    }
}

// SAFETY: HeaderSource has the same private construction and Owner drop order;
// queued payloads release before the header pool, on the owning thread.
unsafe impl<'d, const ID: u8> recycle::raw::PoolOwner<'d, resources::HeaderStorage<'d, ID>>
    for resources::HeaderSource<'d, ID>
{
    fn pool(self) -> ptr::NonNull<recycle::Pool<resources::HeaderStorage<'d, ID>>> {
        self.pool
    }
}

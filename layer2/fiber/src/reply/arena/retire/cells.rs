use o3::{
    alloc, depot,
    depot::{capacity, queue::ring},
};

#[repr(transparent)]
pub(in crate::reply) struct Cell<T>(ring::Cell<T>);

impl<T> Cell<T> {
    pub(in crate::reply) fn try_new(capacity: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self(ring::Cell::try_new(capacity)?))
    }

    pub(in crate::reply) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(in crate::reply) fn pop_front(&self) -> Option<T> {
        self.0.pop_front()
    }

    pub(in crate::reply) fn push(&self, value: T) {
        // SAFETY: both retirement owners allocate one slot per distinct affine
        // source value, so their outstanding values cannot exceed capacity.
        unsafe { depot::raw::RingCell::insert::<false>(&self.0, value) };
    }
}

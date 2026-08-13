mod sealed;
mod tests;

pub(crate) use sealed::retained_context;

#[global_allocator]
static ALLOCATOR: dope_test::checks::TrackingAlloc = dope_test::checks::TrackingAlloc::new();

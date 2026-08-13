mod tests;

#[global_allocator]
static ALLOCATOR: dope_test::checks::TrackingAlloc = dope_test::checks::TrackingAlloc::new();

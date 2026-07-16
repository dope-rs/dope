mod tests;

#[global_allocator]
static ALLOCATOR: dope_test::TrackingAlloc = dope_test::TrackingAlloc;

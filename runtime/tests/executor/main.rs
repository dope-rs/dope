mod client;
mod shutdown;
mod storage;
mod turns;

#[global_allocator]
static ALLOCATOR: dope_test::checks::TrackingAlloc = dope_test::checks::TrackingAlloc::new();

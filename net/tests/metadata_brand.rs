#[test]
fn detached_front_keeps_the_region_exclusive() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/egress/detached_front_reentry.rs");
    cases.compile_fail("tests/ui/egress/stable_bytes_is_sealed.rs");
}

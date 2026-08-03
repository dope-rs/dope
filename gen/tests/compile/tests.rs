#[test]
fn rejects_unsupported_fibers() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/brand/scope_mismatch.rs");
    tests.compile_fail("tests/ui/fiber/*.rs");
}

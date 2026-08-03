#[test]
fn rejects_invalid_fiber_usage() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/file/*.rs");
    tests.compile_fail("tests/ui/local/*.rs");
    tests.compile_fail("tests/ui/task/*.rs");
    tests.compile_fail("tests/ui/timer/*.rs");
}

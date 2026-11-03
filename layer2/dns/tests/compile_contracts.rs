//! Resolver lists require checked selection instead of unchecked indexing.
//!
//! ```compile_fail,E0608
//! use dope_dns::config::Servers;
//! let address = Servers::empty()[0];
//! ```

#[test]
fn indexing_requires_checked_lookup() -> Result<(), dope_test::checks::Error> {
    let suite = dope_test::checks::Suite::new(env!("CARGO_MANIFEST_DIR"), file!(), &["dope-dns"]);
    let exe = std::env::current_exe().expect("current test executable");
    let output = suite
        .try_build(&exe, env!("CARGO_PKG_NAME"))?
        .output()
        .expect("run cargo");
    let suite = suite.try_collect(&exe, output)?;
    assert_eq!(
        suite.try_errors(include_str!("compile_contracts.rs"), compile)?,
        1
    );
    Ok(())
}

fn compile(command: &mut std::process::Command, source: &str) -> std::process::Output {
    let mut child = command.spawn().expect("start compiler");
    let written = child
        .stdin
        .take()
        .map(|mut input| std::io::Write::write_all(&mut input, source.as_bytes()));
    let output = child.wait_with_output().expect("reap compiler");
    written
        .expect("compiler stdin")
        .expect("write compiler source");
    output
}

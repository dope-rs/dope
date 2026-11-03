use std::env;

use dope_test::checks::{Error, Suite};

#[test]
fn source_admission() -> Result<(), Error> {
    let suite = Suite::new(env!("CARGO_MANIFEST_DIR"), file!(), &["dope-core"]);
    let exe = env::current_exe().expect("current test executable");
    let output = suite
        .try_build(&exe, env!("CARGO_PKG_NAME"))?
        .output()
        .expect("run cargo");
    let artifacts = suite.try_collect(&exe, output)?;
    for (source, expected) in [
        (include_str!("common/contract/source_positive.rs"), None),
        (
            include_str!("common/contract/source_unit.rs"),
            Some(("error[E0277]", 2)),
        ),
        (
            include_str!("common/contract/source_reentry.rs"),
            Some(("error[E0599]", 2)),
        ),
        (
            include_str!("common/contract/source_deadline.rs"),
            Some(("lifetime may not live long enough", 1)),
        ),
        (
            include_str!("common/contract/source_deadline_narrow.rs"),
            Some(("lifetime may not live long enough", 1)),
        ),
    ] {
        let output = compile(&mut artifacts.compiler(), source);
        let diag = String::from_utf8(output.stderr).expect("compiler diagnostic");
        assert_eq!(output.status.success(), expected.is_none(), "{diag}");
        if let Some((message, count)) = expected {
            assert_eq!(diag.matches(message).count(), count, "{diag}");
        }
    }
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

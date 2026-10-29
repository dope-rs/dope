use std::{env, io::Write as _, process::Stdio};

use dope_test::checks::{Error, Suite};

#[test]
fn source_admission() -> Result<(), Error> {
    let suite = Suite::new(env!("CARGO_MANIFEST_DIR"), file!(), &["dope-core"]);
    let exe = env::current_exe()?;
    assert!(
        suite
            .try_build(&exe, env!("CARGO_PKG_NAME"))?
            .status()?
            .success()
    );
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
        let mut compiler = suite
            .try_compiler(&exe)?
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        compiler
            .stdin
            .take()
            .expect("compiler stdin")
            .write_all(source.as_bytes())?;
        let output = compiler.wait_with_output()?;
        let diag = String::from_utf8(output.stderr).expect("compiler diagnostic");
        assert_eq!(output.status.success(), expected.is_none(), "{diag}");
        if let Some((message, count)) = expected {
            assert_eq!(diag.matches(message).count(), count, "{diag}");
        }
    }
    Ok(())
}

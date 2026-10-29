use std::{env, io::Write, process::Stdio};

use dope_test::checks::{Error, Suite};

pub mod common;

#[test]
fn fibers_and_pinned_owners() -> Result<(), Error> {
    let exe = env::current_exe()?;
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &[
            "dope-gen",
            "fibers=dope-fiber",
            "engine=dope",
            "pin-project",
        ],
    );
    let status = suite.try_build(&exe, "dope-gen")?.status()?;
    assert!(status.success(), "{status}");

    let mut failures = Vec::new();
    for (source, expected) in common::compile::cases() {
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
        let diag = String::from_utf8(output.stderr).expect("diagnostic UTF-8");
        if !(output.status.success() == expected.is_empty()
            && expected.iter().all(|expected| diag.contains(expected)))
        {
            failures.push(format!("{source}\n{diag}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
    Ok(())
}

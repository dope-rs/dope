use std::env;

#[cfg(test)]
use dope_test::checks::{Error, Suite};

#[cfg(test)]
pub mod common;

#[test]
fn fibers_and_pinned_owners() -> Result<(), Error> {
    let exe = env::current_exe().expect("current test executable");
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
    let output = suite
        .try_build(&exe, "dope-gen")?
        .output()
        .expect("run cargo");
    let suite = suite.try_collect(&exe, output)?;

    for (source, expected) in common::compile::cases() {
        let output = compile(&mut suite.compiler(), &source);
        let diag = String::from_utf8(output.stderr).expect("diagnostic UTF-8");
        assert!(
            output.status.success() == expected.is_empty()
                && expected.iter().all(|expected| diag.contains(expected)),
            "{source}\n{diag}"
        );
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

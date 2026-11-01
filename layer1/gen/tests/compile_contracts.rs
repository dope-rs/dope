use std::env;

#[cfg(test)]
use dope_test::checks::{Error, Suite};

#[cfg(test)]
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
    let output = suite.try_build(&exe, "dope-gen")?.output()?;
    let suite = suite.try_collect(&exe, output)?;

    for (source, expected) in common::compile::cases() {
        let output = suite.try_compile(&source)?;
        let diag = String::from_utf8(output.stderr).expect("diagnostic UTF-8");
        assert!(
            output.status.success() == expected.is_empty()
                && expected.iter().all(|expected| diag.contains(expected)),
            "{source}\n{diag}"
        );
    }
    Ok(())
}

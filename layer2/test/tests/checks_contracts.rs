pub mod common;

use std::{env, ffi::OsStr};

use common::checks::{
    self, ARTIFACT, FOREIGN, INVALID, MIXED_ERRORS, NATIVE, NATIVE_PATH, PACKAGES, PATH,
};
use dope_test::checks::{Error, Suite};

#[test]
fn selects_exact_artifacts() -> Result<(), Error> {
    let exe = env::current_exe().expect("current test executable");
    let suite = Suite::new(env!("CARGO_MANIFEST_DIR"), file!(), PACKAGES);
    let actual = Suite {
        packages: &["dope-test", "o3"],
        ..suite
    };
    let output = actual
        .try_build(&exe, env!("CARGO_PKG_NAME"))?
        .output()
        .expect("run cargo");
    actual.try_collect(&exe, output)?;
    let output = checks::output(&format!("{NATIVE_PATH}\n{ARTIFACT}\n{ARTIFACT}\n"));
    let artifacts = suite.try_collect(&exe, output)?;
    let compiler = artifacts.compiler();
    let args = compiler.get_args().collect::<Vec<_>>();
    assert!(args.contains(&OsStr::new(PATH)));
    assert!(args.contains(&OsStr::new(NATIVE)));
    assert!(args.contains(&OsStr::new("-Znext-solver=globally")));
    assert!(args.contains(&OsStr::new("-Cpanic=abort")));
    let rustdoc = artifacts.try_rustdoc()?;
    assert!(
        rustdoc
            .get_args()
            .any(|arg| arg == "--doctest-build-arg=-Znext-solver=globally")
    );
    assert!(
        !rustdoc
            .get_args()
            .any(|arg| arg == "-Znext-solver=globally")
    );
    Ok(())
}

#[test]
fn rejects_ambiguous_missing_and_invalid_artifacts() -> Result<(), Error> {
    let exe = env::current_exe().expect("current test executable");
    let suite = || Suite::new(env!("CARGO_MANIFEST_DIR"), file!(), PACKAGES);
    assert!(matches!(
        suite().try_collect(&exe, checks::output(&format!("{ARTIFACT}\n{FOREIGN}\n"))),
        Err(Error::Ambiguous { .. })
    ));
    assert!(matches!(
        suite().try_collect(&exe, checks::output(NATIVE_PATH)),
        Err(Error::Missing(_))
    ));
    for source in INVALID {
        assert!(
            matches!(
                suite().try_collect(&exe, checks::output(source)),
                Err(Error::Message { .. })
            ),
            "{source}"
        );
    }
    let artifacts = Suite::new(env!("CARGO_MANIFEST_DIR"), file!(), &[])
        .try_collect(&exe, checks::output(""))?;
    let error = artifacts
        .try_errors(MIXED_ERRORS, compile)
        .expect_err("unrelated compiler errors");
    let Error::Contract { output, .. } = error else {
        panic!("unexpected error: {error:?}");
    };
    let diagnostics = std::str::from_utf8(&output.stderr)?;
    assert!(diagnostics.contains("error[E0308]:"));
    assert!(diagnostics.contains("error[E0425]:"));
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

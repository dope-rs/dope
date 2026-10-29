use std::{env, io::Write, process::Stdio};

use dope_test::checks::{Error, Suite};

#[test]
fn consuming_resume_requires_unpin() -> Result<(), Error> {
    let suite = Suite::new(env!("CARGO_MANIFEST_DIR"), file!(), &["dope-fiber"]);
    let exe = env::current_exe()?;
    let status = suite.try_build(&exe, env!("CARGO_PKG_NAME"))?.status()?;
    assert!(status.success(), "build original library: {status}");
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
        .write_all(include_str!("common/compile_resume_unpin.rs").as_bytes())?;
    let output = compiler.wait_with_output()?;
    let message = String::from_utf8(output.stderr).expect("compiler diagnostics");
    assert!(
        !output.status.success()
            && message.contains("error[E0277]")
            && message.contains("PhantomPinned")
            && message.contains("Unpin"),
        "consuming resume requires Unpin: {message}"
    );
    Ok(())
}

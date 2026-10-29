pub mod common;

use std::{
    env,
    io::{self, Write as _},
    path::Path,
    process::{Command, Stdio},
};

use common::ring::Token;
use dope_sys::io::reactor::submit::Control;
use dope_uring::Uring;

trait Ambiguous<A> {
    const CHECK: () = ();
}

impl<T: ?Sized> Ambiguous<()> for T {}
impl<T: ?Sized + Send> Ambiguous<u8> for T {}
impl<T: ?Sized + Sync> Ambiguous<u16> for T {}

#[test]
fn preserves_borrows() -> io::Result<()> {
    let () = <<Uring as Control>::Poll<'static, Token> as Ambiguous<_>>::CHECK;
    let () = <dope_sys::io::file::Dir<Uring> as Ambiguous<_>>::CHECK;

    let exe = env::current_exe()?;
    let outputs = exe
        .ancestors()
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name == "deps" || name == "build")
        })
        .and_then(Path::parent)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "cargo profile"))?;
    let profile = outputs
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "cargo profile name"))?;
    let profile = if profile == "debug" { "dev" } else { profile };
    let target = outputs
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "cargo target"))?;
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let status = Command::new("rustup")
        .env_remove("RUSTC")
        .args(["run", "stable", "cargo"])
        .args(["build", "--offline", "--locked", "--lib", "--profile"])
        .arg(profile)
        .arg("--config")
        .arg(format!("profile.{profile}.panic=\"abort\""))
        .args([
            "-p",
            "dope-uring",
            "-p",
            "dope-sys",
            "-p",
            "o3",
            "-p",
            "libc",
            "--target-dir",
        ])
        .arg(target)
        .arg("--manifest-path")
        .arg(manifest.join("Cargo.toml"))
        .current_dir(manifest)
        .status()?;
    assert!(status.success(), "contract build: {status}");

    for (source, expected) in [
        (include_str!("common/borrowed.rs"), ""),
        (
            include_str!("common/borrowed_connect.rs"),
            "error: lifetime may not live long enough",
        ),
        (include_str!("common/borrowed_open.rs"), "error[E0515]:"),
        (include_str!("common/borrowed_rename.rs"), "error[E0515]:"),
        (
            include_str!("common/borrowed_unlink.rs"),
            "error: lifetime may not live long enough",
        ),
        (include_str!("common/borrowed_pending.rs"), "error[E0382]:"),
        (
            include_str!("common/borrowed_fifo.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/borrowed_session.rs"),
            "error[E0624]: method `ring` is private",
        ),
        (include_str!("common/borrowed_escape.rs"), "error[E0515]:"),
        (
            include_str!("common/borrowed_drop.rs"),
            "error[E0505]: cannot move out of `file` because it is borrowed",
        ),
    ] {
        let mut compiler = Command::new("rustup");
        compiler
            .args([
                "run",
                "stable",
                "rustc",
                "--edition=2024",
                "--crate-name=uring_borrowed_contract",
                "--crate-type=lib",
                "-Cpanic=abort",
                "--emit=llvm-ir=-",
                "--color=never",
                "-L",
            ])
            .arg(format!("dependency={}", outputs.join("deps").display()))
            .arg("--out-dir")
            .arg(outputs)
            .current_dir(manifest.join("tests/common"));
        for name in ["dope_uring", "dope_sys", "o3", "libc"] {
            compiler.arg("--extern").arg(format!(
                "{name}={}",
                outputs.join(format!("lib{name}.rlib")).display(),
            ));
        }
        let mut child = compiler
            .arg("-")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .expect("compiler input")
            .write_all(source.as_bytes())?;
        let output = child.wait_with_output()?;
        let diag = String::from_utf8(output.stderr).expect("diagnostic UTF-8");
        assert_eq!(
            output.status.success(),
            expected.is_empty(),
            "{source}\n{diag}"
        );
        if !expected.is_empty() {
            assert!(diag.contains(expected), "{source}\n{diag}");
            for error in diag.lines().filter(|line| {
                line.starts_with("error") && !line.starts_with("error: aborting due to")
            }) {
                assert!(error.contains(expected), "{source}\n{diag}");
            }
        }
    }
    Ok(())
}

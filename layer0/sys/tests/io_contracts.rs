pub mod common;

use std::{
    env,
    io::{self, Write as _},
    path::Path,
    process::{Command, Stdio},
};

#[test]
fn preserves_io_ownership() -> io::Result<()> {
    let exe = env::current_exe()?;
    let outputs = exe
        .ancestors()
        .find(|path| path.file_name().is_some_and(|name| name == "deps"))
        .and_then(Path::parent)
        .ok_or_else(|| io::Error::other("cargo profile"))?;
    let profile = outputs
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::other("cargo profile name"))?;
    let profile = if profile == "debug" { "dev" } else { profile };
    let target = outputs
        .parent()
        .ok_or_else(|| io::Error::other("cargo target"))?;
    let toolchain = option_env!("RUSTUP_TOOLCHAIN").map(|value| ("RUSTUP_TOOLCHAIN", value));
    let status = Command::new(env!("CARGO"))
        .env("RUSTC_BOOTSTRAP", "1")
        .envs(toolchain)
        .args([
            "build",
            "--offline",
            "--locked",
            "--lib",
            "-p",
            "dope-sys",
            "--profile",
            profile,
        ])
        .arg("--config")
        .arg(format!("profile.{profile}.panic=\"abort\""))
        .arg("--target-dir")
        .arg(target)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()?;
    assert!(status.success(), "contract build: {status}");

    let mut command = Command::new(option_env!("RUSTC").unwrap_or("rustc"));
    command
        .env("RUSTC_BOOTSTRAP", "1")
        .envs(toolchain)
        .args([
            "-Znext-solver=globally",
            "--edition=2024",
            "--crate-name=sys_io_contract",
            "--crate-type=lib",
            "-Cpanic=abort",
            "--emit=llvm-ir=-",
            "--color=never",
            "-L",
        ])
        .arg(format!("dependency={}", outputs.join("deps").display()))
        .arg("--extern")
        .arg(format!(
            "dope_sys={}",
            outputs.join("libdope_sys.rlib").display()
        ))
        .arg("--out-dir")
        .arg(outputs)
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());

    for (source, expected) in common::io_contracts::CASES {
        let mut compiler = command.spawn()?;
        let input = compiler
            .stdin
            .take()
            .expect("compiler input")
            .write_all(source.as_bytes());
        let output = compiler.wait_with_output()?;
        input?;
        let diag = String::from_utf8(output.stderr).expect("diagnostic UTF-8");
        assert_eq!(
            output.status.success(),
            expected.is_none(),
            "{source}\n{diag}"
        );
        if let Some(expected) = &expected {
            assert!(
                diag.lines().any(|line| line.starts_with(expected)),
                "{source}\n{diag}"
            );
        }
    }
    Ok(())
}

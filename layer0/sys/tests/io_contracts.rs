//! Directory names retain the stream borrow until their last use.
//!
//! ```
//! fn sequential<S: dope_sys::io::file::raw::Directory>(entries: &mut dope_sys::io::file::Entries<'_, S>) {
//!     let first = entries.try_next::<dope_sys::Errno>()
//!         .expect("entry read").expect("entry");
//!     assert!(!first.name.to_bytes().is_empty());
//!     let next = entries.try_next::<dope_sys::Errno>();
//!     assert!(next.is_ok());
//! }
//! ```
//!
//! ```compile_fail,E0499
//! fn overlap<S: dope_sys::io::file::raw::Directory>(entries: &mut dope_sys::io::file::Entries<'_, S>) {
//!     let first = entries.try_next::<dope_sys::Errno>()
//!         .expect("entry read").expect("entry");
//!     let next = entries.try_next::<dope_sys::Errno>();
//!     assert!(!first.name.to_bytes().is_empty());
//!     assert!(next.is_ok());
//! }
//! ```
//!
//! Only the host can construct a socket admission.
//!
//! ```compile_fail,E0451
//! use dope_sys::io::socket::option::tuning::device::Admission;
//!
//! fn fabricate(backend: &mut (), file: u8) -> Admission<'_, (), u8, u32, u32> {
//!     Admission { backend, file, slot: 0, index: 0 }
//! }
//! ```
//!
//! A socket admission can be consumed once.
//!
//! ```
//! use dope_sys::io::socket::option::tuning::device::Admission;
//!
//! fn once(admission: Admission<'_, (), u8, u32, u32>) {
//!     let _ = admission.into_parts();
//! }
//! ```
//!
//! ```compile_fail,E0382
//! use dope_sys::io::socket::option::tuning::device::Admission;
//!
//! fn twice(admission: Admission<'_, (), u8, u32, u32>) {
//!     let first = admission.into_parts();
//!     let second = admission.into_parts();
//! }
//! ```
//!
//! Completion capabilities retain each buffer's original return authority.
//!
//! ```
//! use dope_sys::{engine, memory::buffer::{self, provided}, ring::ready};
//!
//! fn producer<P: provided::Producer>() {
//!     let _ = |owner: &mut P, _token: P::Pending| owner.flush();
//! }
//!
//! fn ready<R: ready::Complete>(source: &mut R, other: &mut R) {
//!     if let Some((_, Some(Ok(token)))) = source.pop() {
//!         let _: R::Pending = token;
//!         other.flush();
//!     }
//! }
//!
//! fn engine<E: engine::Complete>() {
//!     let _ = |owner: &mut E, _token: E::Pending| owner.next();
//! }
//!
//! fn release<R: buffer::raw::Release<T>, T>() {
//!     let _ = |owner: &mut R, token: T| unsafe { owner.release(token) };
//! }
//! ```
//!
//! ```compile_fail,E0599
//! use dope_sys::memory::buffer::provided;
//!
//! fn defer<P: provided::Producer>() {
//!     let _ = |owner: &mut P, token: P::Pending| owner.defer(token);
//! }
//! ```
//!
//! ```compile_fail,E0599
//! use dope_sys::ring::ready;
//!
//! fn defer<R: ready::Complete>(source: &mut R, other: &mut R) {
//!     if let Some((_, Some(Ok(token)))) = source.pop() {
//!         other.defer(token);
//!     }
//! }
//! ```
//!
//! ```compile_fail,E0599
//! use dope_sys::engine;
//!
//! fn release<E: engine::Complete>() {
//!     let _ = |owner: &mut E, token: E::Pending| owner.release(token);
//! }
//! ```
//!
//! ```compile_fail,E0133
//! use dope_sys::memory::buffer;
//!
//! fn release<R: buffer::raw::Release<T>, T>() {
//!     let _ = |owner: &mut R, token: T| owner.release(token);
//! }
//! ```

use std::{
    env,
    io::{self, Write as _},
    path::Path,
    process::{Command, Stdio},
};

#[test]
fn preserves_io_ownership() -> io::Result<()> {
    let exe = env::current_exe()?;
    let target = exe
        .ancestors()
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name == "deps" || name == "build")
        })
        .and_then(Path::parent)
        .and_then(Path::parent)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "cargo target"))?;
    let tests = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let status = Command::new("rustup")
        .env_remove("RUSTC")
        .args([
            "run",
            "stable",
            "cargo",
            "--config",
            "profile.dev.panic=\"abort\"",
            "build",
            "--offline",
            "--locked",
            "--lib",
            "-p",
            "dope-sys",
            "--target-dir",
        ])
        .arg(target)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()?;
    assert!(status.success(), "contract build: {status}");
    let outputs = target.join("debug");
    let mut blocks = include_str!("io_contracts.rs").split("\n//! ```").skip(1);
    let mut checked = [0, 0];
    while let Some(block) = blocks.next() {
        assert!(blocks.next().expect("closing code fence").starts_with('\n'));
        let (annotation, body) = block.split_once('\n').expect("contract body");
        let expected = if annotation.is_empty() {
            String::new()
        } else {
            let code = annotation
                .strip_prefix("compile_fail,")
                .expect("error code");
            format!("error[{code}]:")
        };
        let mut source = String::new();
        for line in body.lines() {
            source.push_str(line.strip_prefix("//!").expect("contract line"));
            source.push('\n');
        }
        let mut compiler = Command::new("rustup")
            .args([
                "run",
                "stable",
                "rustc",
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
            .arg(&outputs)
            .arg("-")
            .current_dir(&tests)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        compiler
            .stdin
            .take()
            .expect("compiler input")
            .write_all(source.as_bytes())?;
        let output = compiler.wait_with_output()?;
        let diag = String::from_utf8(output.stderr).expect("diagnostic UTF-8");
        assert_eq!(
            output.status.success(),
            expected.is_empty(),
            "{source}\n{diag}"
        );
        if !expected.is_empty() {
            assert!(
                diag.lines().any(|line| line.starts_with(&expected)),
                "{source}\n{diag}"
            );
        }
        checked[usize::from(!expected.is_empty())] += 1;
    }
    assert_eq!(checked, [3, 7]);
    Ok(())
}

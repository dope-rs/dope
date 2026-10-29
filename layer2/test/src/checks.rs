use std::{
    env::consts::DLL_EXTENSION,
    io::{self, Write as _},
    path::Path,
    process::{Command, Output, Stdio},
};

use dope_core::{
    Backend, Errno,
    dope_sys::io::file::{self, File as _, Mode, Open},
    io::fs::path::{CStrExt as _, CopyError},
};

/// Checks contracts against the consuming crate's original build artifacts.
#[derive(Debug)]
pub struct Suite<'a> {
    pub manifest: &'a str,
    /// Rustup override; empty inherits the recorded Cargo and toolchain.
    pub toolchain: &'a str,
    pub source: &'a str,
    /// Cargo packages, optionally `alias=package` for renamed externs.
    pub packages: &'a [&'a str],
}

impl<'a> Suite<'a> {
    pub const fn new(manifest: &'a str, source: &'a str, packages: &'a [&'a str]) -> Self {
        Self {
            manifest,
            toolchain: "",
            source,
            packages,
        }
    }

    pub fn try_build(&self, exe: &Path, package: &str) -> Result<Command, Error> {
        let outputs = Self::outputs(exe)?;
        let target = outputs.parent().ok_or(Error::Target)?;
        let profile = outputs
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(Error::Target)?;
        let profile = if profile == "debug" { "dev" } else { profile };
        let name = Path::new(self.source).file_stem().ok_or(Error::Target)?;
        let mut cargo = self.command("cargo");
        cargo
            .args(["build", "--offline", "--locked", "--lib", "--test"])
            .arg(name)
            .args(["--profile", profile])
            .arg("--target-dir")
            .arg(target)
            .args(["-p", package]);
        for package in self.packages {
            let (_, package) = package.split_once('=').unwrap_or((package, package));
            cargo.args(["-p", package]);
        }
        Ok(cargo)
    }

    pub fn try_rustdoc(&self, exe: &Path) -> Result<Command, Error> {
        let outputs = Self::outputs(exe)?;
        let source = Path::new(self.source).file_name().ok_or(Error::Target)?;
        let tests = Path::new(self.manifest).join("tests");
        let mut rustdoc = self.command("rustdoc");
        rustdoc
            .arg("--test")
            .arg(tests.join(source))
            .args(["--edition=2024", "-Cpanic=abort", "--test-run-directory"])
            .arg(&tests);
        self.libraries(&mut rustdoc, outputs)?;
        Ok(rustdoc)
    }

    /// Compiles an original fixture supplied on stdin without creating a source file.
    pub fn try_compiler(&self, exe: &Path) -> Result<Command, Error> {
        let outputs = Self::outputs(exe)?;
        let mut compiler = self.command("rustc");
        compiler.args([
            "-Znext-solver=globally",
            "--edition=2024",
            "--crate-name=dope_compile_contract",
            "--crate-type=bin",
            "-Cpanic=abort",
            "--emit=llvm-ir=-",
            "--color=never",
        ]);
        compiler.arg("--out-dir").arg(outputs);
        self.libraries(&mut compiler, outputs)?;
        compiler.arg("-");
        Ok(compiler)
    }

    /// Checks the original `compile_fail,E####` doc blocks that stable rustdoc treats
    /// as plain text. Other doctests remain owned by `try_rustdoc`.
    pub fn try_errors(&self, exe: &Path, source: &str) -> Result<usize, Error> {
        let mut lines = source.lines().enumerate();
        let mut count = 0;
        while let Some((line, text)) = lines.next() {
            let Some(header) = Self::doc(text).and_then(|text| text.strip_prefix("```")) else {
                continue;
            };
            if matches!(header, "" | "compile_fail") {
                continue;
            }
            let line = line + 1;
            let code = header
                .strip_prefix("compile_fail,")
                .filter(|code| {
                    code.len() == 5
                        && code.starts_with('E')
                        && code.as_bytes()[1..].iter().all(u8::is_ascii_digit)
                })
                .ok_or(Error::Fixture { line })?;
            let mut body = String::new();
            let mut main = false;
            let mut closed = false;
            for (at, text) in lines.by_ref() {
                let text = Self::doc(text).ok_or(Error::Fixture { line: at + 1 })?;
                if text == "```" {
                    closed = true;
                    break;
                }
                let text = match text {
                    "#" => "",
                    text => text.strip_prefix("# ").unwrap_or(text),
                };
                main |= text.trim_start().starts_with("fn main(");
                body.push_str(text);
                body.push('\n');
            }
            if !closed {
                return Err(Error::Fixture { line });
            }
            if !main {
                body.insert_str(0, "fn main() {\n");
                body.push_str("}\n");
            }
            let mut child = self
                .try_compiler(exe)?
                .current_dir(Path::new(self.manifest).join("tests"))
                .env("CARGO_MANIFEST_DIR", self.manifest)
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()?;
            let input = match child.stdin.take() {
                Some(mut input) => input.write_all(body.as_bytes()).map_err(Error::Io),
                None => Err(Error::Input),
            };
            let output = child.wait_with_output()?;
            input?;
            let expected = format!("error[{code}]:");
            if output.status.success()
                || !output
                    .stderr
                    .split(|byte| *byte == b'\n')
                    .any(|line| line.starts_with(expected.as_bytes()))
            {
                return Err(Error::Contract {
                    line,
                    expected,
                    output,
                });
            }
            count += 1;
        }
        Ok(count)
    }

    fn doc(text: &str) -> Option<&str> {
        let text = text.trim_start();
        let text = text
            .strip_prefix("//!")
            .or_else(|| text.strip_prefix("///"))?;
        Some(text.strip_prefix(' ').unwrap_or(text))
    }

    fn outputs(exe: &Path) -> Result<&Path, Error> {
        exe.ancestors()
            .find(|path| {
                path.file_name()
                    .is_some_and(|name| name == "deps" || name == "build")
            })
            .and_then(Path::parent)
            .ok_or(Error::Target)
    }

    fn libraries(&self, command: &mut Command, outputs: &Path) -> Result<(), Error> {
        Self::search(command, &outputs.join("deps"));
        Self::directories(command, &outputs.join("build"), 2)?;
        for package in self.packages {
            let (alias, package) = package.split_once('=').unwrap_or((package, package));
            let name = alias.replace('-', "_");
            let libname = package.replace('-', "_");
            let mut library = outputs.join(format!("lib{libname}.rlib"));
            if !Self::try_exists(&library)? {
                library.set_extension(DLL_EXTENSION);
                if !Self::try_exists(&library)? {
                    return Err(Error::Target);
                }
            }
            command
                .arg("--extern")
                .arg(format!("{name}={}", library.display()));
        }
        Ok(())
    }

    fn try_exists(path: &Path) -> Result<bool, Error> {
        let path = Box::try_copy(path).map_err(Error::Path)?;
        match Backend::stat::<Errno>(&path) {
            Ok(_) => Ok(true),
            Err(error) if error == Errno::NOT_FOUND => Ok(false),
            Err(error) => Err(Error::Syscall(error)),
        }
    }

    fn command(&self, tool: &str) -> Command {
        let mut command = if self.toolchain.is_empty() {
            let exe = match tool {
                "cargo" => env!("CARGO"),
                "rustdoc" => option_env!("RUSTDOC").unwrap_or(tool),
                _ => option_env!("RUSTC").unwrap_or(tool),
            };
            let mut command = Command::new(exe);
            if let Some(chain) = option_env!("RUSTUP_TOOLCHAIN") {
                command.env("RUSTUP_TOOLCHAIN", chain);
            }
            command
        } else {
            let mut command = Command::new("rustup");
            command.args(["run", self.toolchain, tool]);
            command
        };
        command.current_dir(self.manifest);
        command
    }

    fn search(command: &mut Command, path: &Path) {
        command
            .arg("-L")
            .arg(format!("dependency={}", path.display()));
    }

    /// Cargo directories retain exact crate hashes; explicit externs select the fresh graph.
    fn directories(command: &mut Command, root: &Path, depth: u8) -> Result<(), Error> {
        let path = Box::try_copy(root).map_err(Error::Path)?;
        let mut dir = match file::Dir::<Backend>::open::<Errno>(&path) {
            Ok(dir) => dir,
            Err(file::OpenError::Syscall(error)) if error == Errno::NOT_FOUND => {
                return Ok(());
            }
            Err(error) => return Err(Error::Directory(error)),
        };
        Self::directory(command, root, depth, &mut dir)
    }

    fn directory(
        command: &mut Command,
        root: &Path,
        depth: u8,
        dir: &mut file::Dir<Backend>,
    ) -> Result<(), Error> {
        use std::os::unix::ffi::OsStrExt as _;

        let (fd, mut entries) = dir.entries();
        while let Some(entry) = entries.try_next::<Errno>().map_err(Error::Read)? {
            if matches!(entry.name.to_bytes(), b"." | b"..") {
                continue;
            }
            let kind = match entry.kind {
                Some(kind) => kind,
                None => Backend::metadata_at::<Errno>(Some(fd), entry.name)
                    .map_err(Error::Syscall)?
                    .kind(),
            };
            if !kind.is_dir() {
                continue;
            }
            let path = root.join(std::ffi::OsStr::from_bytes(entry.name.to_bytes()));
            if entry.name == c"out" {
                Self::search(command, &path);
            } else if depth > 0 {
                let child = match Backend::open_at::<Errno>(
                    fd,
                    entry.name,
                    Open::DIRECTORY | Open::NO_FOLLOW | Open::CLOSE_ON_EXEC,
                    Mode::new(0),
                ) {
                    Ok(child) => child,
                    Err(error) if error == Errno::NOT_FOUND => continue,
                    Err(error) => return Err(Error::Syscall(error)),
                };
                let mut child =
                    file::Dir::<Backend>::from_fd::<Errno>(child).map_err(Error::Directory)?;
                Self::directory(command, &path, depth - 1, &mut child)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Input,
    Fixture {
        line: usize,
    },
    Contract {
        line: usize,
        expected: String,
        output: Output,
    },
    Path(CopyError),
    Directory(file::OpenError<Errno>),
    Read(file::ReadError<Errno>),
    Syscall(Errno),
    Target,
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

use std::{
    env::consts::DLL_EXTENSION,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};

mod message;

use message::Value;

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

    /// Selects the test's dependency graph, including its library and panic profile.
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
            .args([
                "build",
                "--offline",
                "--locked",
                "--message-format=json",
                "--test",
            ])
            .arg(name)
            .args(["--profile", profile])
            .arg("--target-dir")
            .arg(target)
            .args(["-p", package]);
        Ok(cargo)
    }

    /// Selects only artifacts reported by this exact Cargo invocation.
    pub fn try_collect(self, exe: &Path, output: Output) -> Result<Artifacts<'a>, Error> {
        if !output.status.success() {
            return Err(Error::Build(output));
        }
        let outputs = Self::outputs(exe)?.to_owned();
        let mut libraries: Vec<Option<(String, PathBuf)>> =
            self.packages.iter().map(|_| None).collect();
        let mut native = Vec::new();
        for line in std::str::from_utf8(&output.stdout)?
            .lines()
            .filter(|line| !line.is_empty())
        {
            let message = Value::parse(line)?;
            match message.field("reason")?.string()?.as_ref() {
                "build-script-executed" => {
                    for path in message.field("linked_paths")?.array()? {
                        let path = path.string()?.into_owned();
                        if !native.contains(&path) {
                            native.push(path);
                        }
                    }
                }
                "compiler-artifact" => {
                    if message.field("profile")?.field("test")?.text != "false" {
                        continue;
                    }
                    let target = message.field("target")?;
                    let kinds = target.field("kind")?.array()?;
                    let kinds = kinds
                        .iter()
                        .map(Value::string)
                        .collect::<Result<Vec<_>, _>>()?;
                    let proc_macro = kinds.iter().any(|kind| kind == "proc-macro");
                    if !proc_macro && !kinds.iter().any(|kind| kind == "lib" || kind == "rlib") {
                        continue;
                    }
                    let name = target.field("name")?.string()?;
                    for (index, package) in self.packages.iter().enumerate() {
                        let (_, package) = package.split_once('=').unwrap_or((package, package));
                        if name != package.replace('-', "_") {
                            continue;
                        }
                        let identity = message.field("package_id")?.string()?;
                        for path in message.field("filenames")?.array()? {
                            let path = PathBuf::from(path.string()?.as_ref());
                            let extension = if proc_macro { DLL_EXTENSION } else { "rlib" };
                            if path.extension().and_then(|ext| ext.to_str()) != Some(extension) {
                                continue;
                            }
                            match &libraries[index] {
                                Some((previous, file))
                                    if previous.as_str() != identity.as_ref() || file != &path =>
                                {
                                    return Err(Error::Ambiguous {
                                        package: package.to_string(),
                                        first: (previous.clone(), file.clone()),
                                        next: (identity.into_owned(), path),
                                    });
                                }
                                Some(_) => {}
                                None => {
                                    libraries[index] = Some((identity.clone().into_owned(), path))
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        let libraries = self
            .packages
            .iter()
            .zip(libraries)
            .map(|(package, library)| {
                library
                    .map(|(_, path)| path)
                    .ok_or_else(|| Error::Missing((*package).to_owned()))
            })
            .collect::<Result<_, _>>()?;
        Ok(Artifacts {
            suite: self,
            outputs,
            libraries,
            native,
        })
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
        command
            .current_dir(self.manifest)
            .env("RUSTC_BOOTSTRAP", "1");
        command
    }
}

#[derive(Debug)]
pub struct Artifacts<'a> {
    suite: Suite<'a>,
    outputs: PathBuf,
    libraries: Vec<PathBuf>,
    native: Vec<String>,
}

impl Artifacts<'_> {
    pub fn try_rustdoc(&self) -> Result<Command, Error> {
        let tests = Path::new(self.suite.manifest).join("tests");
        let mut rustdoc = self.suite.command("rustdoc");
        rustdoc
            .args([
                "--test",
                "-Zunstable-options",
                "--doctest-build-arg=-Znext-solver=globally",
            ])
            .arg(
                Path::new(self.suite.source)
                    .file_name()
                    .ok_or(Error::Target)?,
            )
            .current_dir(&tests)
            .env("CARGO_MANIFEST_DIR", self.suite.manifest)
            .args(["--edition=2024", "-Cpanic=abort", "--test-run-directory"])
            .arg(&tests);
        self.libraries(&mut rustdoc);
        Ok(rustdoc)
    }

    pub fn compiler(&self) -> Command {
        let mut compiler = self.suite.command("rustc");
        compiler.args([
            "-Znext-solver=globally",
            "--edition=2024",
            "--crate-name=dope_compile_contract",
            "--crate-type=bin",
            "-Cpanic=abort",
            "--emit=llvm-ir=-",
            "--color=never",
        ]);
        compiler
            .current_dir(Path::new(self.suite.manifest).join("tests"))
            .env("CARGO_MANIFEST_DIR", self.suite.manifest)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        self.libraries(&mut compiler);
        compiler.arg("-");
        compiler
    }

    /// Checks the original `compile_fail,E####` doc blocks that stable rustdoc treats
    /// as plain text. The caller supplies compiler execution; `try_rustdoc` owns other doctests.
    pub fn try_errors(
        &self,
        source: &str,
        mut run: impl FnMut(&mut Command, &str) -> Output,
    ) -> Result<usize, Error> {
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
            let output = run(&mut self.compiler(), &body);
            let expected = format!("error[{code}]:");
            let mut errors = output.stderr.split(|byte| *byte == b'\n').filter(|line| {
                line.starts_with(b"error") && !line.starts_with(b"error: aborting due to")
            });
            let matches = errors
                .next()
                .is_some_and(|line| line.starts_with(expected.as_bytes()))
                && errors.all(|line| line.starts_with(expected.as_bytes()));
            if output.status.success() || !matches {
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

    fn libraries(&self, command: &mut Command) {
        command.arg("-L").arg(format!(
            "dependency={}",
            self.outputs.join("deps").display()
        ));
        for path in &self.native {
            command.arg("-L").arg(path);
        }
        for (package, path) in self.suite.packages.iter().zip(&self.libraries) {
            let (alias, _) = package.split_once('=').unwrap_or((package, package));
            command
                .arg("--extern")
                .arg(format!("{}={}", alias.replace('-', "_"), path.display()));
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Utf8(std::str::Utf8Error),
    Message {
        offset: usize,
    },
    Field(&'static str),
    Build(Output),
    Missing(String),
    Ambiguous {
        package: String,
        first: (String, PathBuf),
        next: (String, PathBuf),
    },
    Fixture {
        line: usize,
    },
    Contract {
        line: usize,
        expected: String,
        output: Output,
    },
    Target,
}

impl From<std::str::Utf8Error> for Error {
    fn from(error: std::str::Utf8Error) -> Self {
        Self::Utf8(error)
    }
}

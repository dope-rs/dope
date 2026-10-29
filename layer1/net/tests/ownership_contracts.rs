use std::{env, io::Write as _, process::Stdio};

use dope_test::checks::{Error, Suite};

#[test]
fn ownership_contracts() -> Result<(), Error> {
    let exe = env::current_exe()?;
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &["dope-net", "dope-core", "o3"],
    );
    let status = suite.try_build(&exe, "dope-net")?.status()?;
    assert!(status.success());

    for (source, expected) in [
        (include_str!("common/ownership/admitted.rs"), ""),
        (
            include_str!("common/ownership/arena_escape.rs"),
            "error[E0515]: cannot return value referencing local variable `store`",
        ),
        (
            include_str!("common/ownership/arena_alias.rs"),
            "error[E0499]: cannot borrow `*store` as mutable more than once at a time",
        ),
        (
            include_str!("common/ownership/lease_detach.rs"),
            "error[E0624]: method `into_parts` is private",
        ),
        (
            include_str!("common/ownership/lease_substitute.rs"),
            "error[E0624]: associated function `new` is private",
        ),
        (
            include_str!("common/ownership/lease_escape.rs"),
            "error[E0309]: the parameter type `R` may not live long enough",
        ),
        (
            include_str!("common/ownership/pair_geometry.rs"),
            "error[E0277]: the trait bound `C: Segment` is not satisfied",
        ),
        (
            include_str!("common/ownership/produced_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/ownership/produced_replay.rs"),
            "error[E0382]: use of moved value: `output`",
        ),
        (
            include_str!("common/ownership/input_consume.rs"),
            "error[E0502]: cannot borrow `*input` as mutable because it is also borrowed as immutable",
        ),
        (
            include_str!("common/ownership/chunk_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/ownership/chunk_credit.rs"),
            "error[E0308]: mismatched types",
        ),
        (
            include_str!("common/ownership/completion_forge.rs"),
            "error[E0423]: cannot initialize a tuple struct which contains private fields",
        ),
        (
            include_str!("common/ownership/prepared_alias.rs"),
            "error[E0499]: cannot borrow `storage` as mutable more than once at a time",
        ),
        (
            include_str!("common/ownership/round_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/ownership/front_escape.rs"),
            "error[E0597]: `arena` does not live long enough",
        ),
        (
            include_str!("common/ownership/batch_rejected_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/ownership/front_reenter.rs"),
            "error[E0499]: cannot borrow `*token` as mutable more than once at a time",
        ),
    ] {
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
        assert!(
            output.status.success() == expected.is_empty() && diag.contains(expected),
            "{source}\n{diag}"
        );
    }
    Ok(())
}

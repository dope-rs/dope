use std::env;

use dope_test::checks::{Error, Suite};

#[test]
fn ownership_contracts() -> Result<(), Error> {
    let exe = env::current_exe().expect("current test executable");
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &["dope-net", "dope-core", "o3"],
    );
    let output = suite
        .try_build(&exe, "dope-net")?
        .output()
        .expect("run cargo");
    let suite = suite.try_collect(&exe, output)?;

    for (source, expected) in [
        ("common/ownership/admitted.rs", ""),
        (
            "common/ownership/arena_escape.rs",
            "error[E0515]: cannot return value referencing local variable `store`",
        ),
        (
            "common/ownership/arena_alias.rs",
            "error[E0499]: cannot borrow `*store` as mutable more than once at a time",
        ),
        (
            "common/ownership/lease_detach.rs",
            "error[E0624]: method `into_parts` is private",
        ),
        (
            "common/ownership/lease_substitute.rs",
            "error[E0624]: associated function `new` is private",
        ),
        (
            "common/ownership/lease_escape.rs",
            "error[E0309]: the parameter type `R` may not live long enough",
        ),
        (
            "common/ownership/pair_geometry.rs",
            "error[E0277]: the trait bound `C: Segment` is not satisfied",
        ),
        (
            "common/ownership/produced_escape.rs",
            "error: lifetime may not live long enough",
        ),
        (
            "common/ownership/produced_replay.rs",
            "error[E0382]: use of moved value: `output`",
        ),
        (
            "common/ownership/input_consume.rs",
            "error[E0502]: cannot borrow `*input` as mutable because it is also borrowed as immutable",
        ),
        (
            "common/ownership/chunk_escape.rs",
            "error: lifetime may not live long enough",
        ),
        (
            "common/ownership/chunk_credit.rs",
            "error[E0308]: mismatched types",
        ),
        (
            "common/ownership/completion_forge.rs",
            "error[E0423]: cannot initialize a tuple struct which contains private fields",
        ),
        (
            "common/ownership/prepared_forge.rs",
            "error[E0624]: associated function `new` is private",
        ),
        (
            "common/ownership/prepared_alias.rs",
            "error[E0382]: use of moved value: `storage`\nerror[E0505]: cannot move out of `storage` because it is borrowed",
        ),
        (
            "common/ownership/round_escape.rs",
            "error: lifetime may not live long enough",
        ),
        (
            "common/ownership/front_escape.rs",
            "error[E0597]: `arena` does not live long enough",
        ),
        (
            "common/ownership/batch_rejected_escape.rs",
            "error: lifetime may not live long enough",
        ),
        (
            "common/ownership/front_reenter.rs",
            "error[E0499]: cannot borrow `*token` as mutable more than once at a time",
        ),
    ] {
        let source_file = std::fs::File::open(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests")
                .join(source),
        )
        .expect("open compiler fixture");
        let output = suite
            .compiler()
            .stdin(source_file)
            .output()
            .expect("run compiler");
        let diag = String::from_utf8(output.stderr).expect("diagnostic UTF-8");
        assert!(
            output.status.success() == expected.is_empty()
                && expected.lines().all(|expected| diag.contains(expected)),
            "{source}\n{diag}"
        );
    }
    Ok(())
}

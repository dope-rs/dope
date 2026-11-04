use std::env;

use dope_test::checks::{Error, Suite};

#[test]
fn ownership_contracts() -> Result<(), Error> {
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &["dope-core", "dope-runtime"],
    );
    let exe = env::current_exe().expect("current test executable");
    let output = suite
        .try_build(&exe, env!("CARGO_PKG_NAME"))?
        .output()
        .expect("run cargo");
    let suite = suite.try_collect(&exe, output)?;
    let output_escape = include_str!("common/contracts/positive.rs")
        .replace("|data, _app| data", "|_data, _app| _client");
    let session_client_escape = include_str!("common/contracts/positive.rs").replace(
        "|data, _app| data",
        "|_data, mut app| app.client(|root| root.tail())",
    );
    let pair_first_escape = include_str!("common/contracts/positive.rs").replace(
        "let _first: Bound<'app, 'd, Value, u8>",
        "let _first: Bound<'d, 'd, Value, u8>",
    );
    let pair_second_escape = include_str!("common/contracts/positive.rs").replace(
        "let _second: Bound<'app, 'd, Value, u8>",
        "let _second: Bound<'d, 'd, Value, u8>",
    );
    let pair_first_loan = include_str!("common/contracts/positive.rs").replace(
        "paired_loans<'first: 'app, 'second: 'app, 'app, 'd: 'app>",
        "paired_loans<'first, 'second: 'app, 'app, 'd: 'app>",
    );
    let pair_second_loan = include_str!("common/contracts/positive.rs").replace(
        "paired_loans<'first: 'app, 'second: 'app, 'app, 'd: 'app>",
        "paired_loans<'first: 'app, 'second, 'app, 'd: 'app>",
    );

    for (source, expected) in [
        (include_str!("common/contracts/positive.rs"), ""),
        (
            concat!(
                include_str!("common/contracts/positive.rs"),
                include_str!("common/contracts/layout_execute.rs"),
            ),
            "error[E0599]",
        ),
        (
            output_escape.as_str(),
            "error: lifetime may not live long enough",
        ),
        (
            session_client_escape.as_str(),
            "error: lifetime may not live long enough",
        ),
        (
            pair_first_escape.as_str(),
            "error: lifetime may not live long enough",
        ),
        (
            pair_second_escape.as_str(),
            "error: lifetime may not live long enough",
        ),
        (
            pair_first_loan.as_str(),
            "error: lifetime may not live long enough",
        ),
        (
            pair_second_loan.as_str(),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/terminal_replay.rs"),
            "error[E0382]: use of moved value: `terminal`",
        ),
        (
            include_str!("common/contracts/terminal_domain.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/terminal_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/hash_state_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/hash_state_forge.rs"),
            "error[E0624]: associated function `for_seed` is private",
        ),
        (
            include_str!("common/contracts/hasher_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/random_root_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/client_forge.rs"),
            "error[E0624]: associated function `new` is private",
        ),
        (
            include_str!("common/contracts/client_retag.rs"),
            "error[E0308]: mismatched types",
        ),
        (
            include_str!("common/contracts/client_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/provider_borrow.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/lending_forge.rs"),
            "error[E0624]: associated function `new` is private",
        ),
        (
            include_str!("common/contracts/storage_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/stage_forge.rs"),
            "error[E0624]: associated function `new` is private",
        ),
        (
            include_str!("common/contracts/shutdown_retag.rs"),
            "error[E0308]: mismatched types",
        ),
        (
            include_str!("common/contracts/app_replay.rs"),
            "error[E0382]: use of moved value: `app`",
        ),
        (
            include_str!("common/contracts/fixed_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/manual_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/root_retag.rs"),
            "error[E0308]: mismatched types",
        ),
        (
            include_str!("common/contracts/root_forge.rs"),
            "error[E0624]: associated function `new` is private",
        ),
        (
            include_str!("common/contracts/root_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/lending_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/lending_recombine.rs"),
            "error[E0624]: associated function `new` is private",
        ),
        (
            include_str!("common/contracts/permit_domain.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/released_forge.rs"),
            "error[E0624]: associated function `new` is private",
        ),
        (
            include_str!("common/contracts/released_rebind.rs"),
            "error[E0133]: call to unsafe function",
        ),
        (
            concat!(
                include_str!("common/contracts/positive.rs"),
                include_str!("common/contracts/lending_resume.rs"),
            ),
            "the trait bound `Value: dope_runtime::topology::lending::raw::Resume<",
        ),
        (
            include_str!("common/contracts/permit_forge.rs"),
            "error[E0624]: associated function `new` is private",
        ),
        (
            include_str!("common/contracts/outcome_forge.rs"),
            "error[E0624]: associated function `executed` is private",
        ),
        (
            include_str!("common/contracts/permit_replay.rs"),
            "error[E0382]: use of moved value: `permit`",
        ),
        (
            include_str!("common/contracts/permit_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/outcome_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/held_forge.rs"),
            "error[E0624]: associated function `new` is private",
        ),
        (
            include_str!("common/contracts/held_replay.rs"),
            "error[E0382]: use of moved value: `held`",
        ),
        (
            include_str!("common/contracts/held_escape.rs"),
            "error: lifetime may not live long enough",
        ),
        (
            include_str!("common/contracts/held_domain.rs"),
            "error: lifetime may not live long enough",
        ),
    ] {
        let output = compile(&mut suite.compiler(), source);
        let diag = String::from_utf8(output.stderr).expect("diagnostic UTF-8");
        assert!(
            output.status.success() == expected.is_empty() && diag.contains(expected),
            "{source}\n{diag}",
        );
    }
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

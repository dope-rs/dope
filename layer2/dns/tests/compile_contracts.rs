//! Resolver lists require checked selection instead of unchecked indexing.
//!
//! ```compile_fail,E0608
//! use dope_dns::config::Servers;
//! let address = Servers::empty()[0];
//! ```

#[test]
fn indexing_requires_checked_lookup() -> Result<(), dope_test::checks::Error> {
    let suite = dope_test::checks::Suite::new(env!("CARGO_MANIFEST_DIR"), file!(), &["dope-dns"]);
    let exe = std::env::current_exe()?;
    let status = suite.try_build(&exe, env!("CARGO_PKG_NAME"))?.status()?;
    assert!(status.success(), "build original library: {status}");
    assert_eq!(
        suite.try_errors(&exe, include_str!("compile_contracts.rs"))?,
        1
    );
    Ok(())
}

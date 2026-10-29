use std::{convert::Infallible, hash::BuildHasher};

use dope_core::driver::life::{creation::Attempt, recovery};
use dope_runtime::random::Entropy;
pub mod common;

use dope_core::{driver::settings, random::Seed};
use dope_runtime::{executor::Executor, random};

use crate::common::{
    random::{HASH_DOMAIN, HASH_INPUT, Purpose},
    scenario::rt::Configured,
};

#[test]
fn preserves_hash_namespace() {
    let seed = Seed::acquire().expect("seed entropy");
    let name = random::HashSeed::from_seed(&seed, HASH_DOMAIN.instance(0));
    let same = random::HashSeed::from_seed(&seed, HASH_DOMAIN.instance(0));
    let exact = random::HashSeed::from_seed(&seed, HASH_DOMAIN.instance(1));
    let input = HASH_INPUT;
    assert_eq!(name.hash_one(input), name.hash(&input));
    assert_eq!(name.hash_one(input), name.clone().hash_one(input));
    assert_eq!(name.hash(&input), name.clone().hash(&input));
    assert_eq!(name.hash(&input), same.hash(&input));
    assert_ne!(name.hash(&input), exact.hash(&input));
    assert_eq!(name.hash_bytes(input.0), name.clone().hash_bytes(input.0));
    assert_eq!(name.hash_bytes(input.0), same.hash_bytes(input.0));
    assert_ne!(name.hash_bytes(input.0), exact.hash_bytes(input.0));
}

#[test]
fn separates_roots() {
    let mut create = Attempt::new();
    let executor = Executor::new(
        &mut create,
        Configured::quic::<2, 8>()
            .config()
            .with_domains(settings::Domain::ONE),
    );
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    let executor = executor.ok().expect("validated executor");
    let mut reports = Vec::new();

    executor.run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let driver =
                    random::Root::<Purpose>::try_acquire(&session.driver()).expect("root identity");
                let root = session.try_root::<Purpose>().expect("root identity");
                assert_eq!(
                    root.word::<0x7061_7468>(17).into_bytes(),
                    root.word::<0x7061_7468>(17).into_bytes()
                );
                assert_ne!(
                    driver.word::<0x7061_7468>(17).into_bytes(),
                    root.word::<0x7061_7468>(17).into_bytes()
                );
                assert_ne!(
                    driver.permute::<0x7061_7468>(17).into_bytes(),
                    root.permute::<0x7061_7468>(17).into_bytes()
                );
                let hash = root.hash_seed::<0x7061_7468>().hash(&HASH_INPUT);
                assert_eq!(hash, root.hash_seed::<0x7061_7468>().hash(&HASH_INPUT));
                assert_ne!(hash, root.hash_seed::<0x7061_7469>().hash(&HASH_INPUT));
                assert_ne!(hash, driver.hash_seed::<0x7061_7468>().hash(&HASH_INPUT));
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(
            reports.is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );

        assert!(result.is_ok(), "random domain must finish");
    });
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}

use std::{io, net};

use dope::{
    core::driver::settings, manifold::timing::Balanced, net::link::egress,
    runtime::executor::Executor,
};
use dope_dns::{Storage, config};

use crate::fixture;

#[test]
fn invalid_outbound_plans_do_not_consume_transport_bindings() -> io::Result<()> {
    let servers = config::Servers::try_from_iter([net::SocketAddr::from(([127, 0, 0, 1], 53))])
        .expect("one DNS server");
    let storage = Storage::<1, 4>::new(fixture::config(servers, 1))?;
    let executor =
        Executor::new(settings::Config::for_tcp_profile::<Balanced>(1)?)?.with_storage(storage);

    executor.enter(|mut session| {
        let seed = session.hash_state(dope_dns::HASH_DOMAIN);
        let storage = session.storage();
        let mut driver = session.driver_access();
        let error = match storage.bind::<0, 1, 0>(seed, egress::Config::default(), &mut driver) {
            Ok(_) => panic!("zero outbound bound was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

        let error = match storage.bind::<0, 1, { usize::MAX }>(
            seed,
            egress::Config::default(),
            &mut driver,
        ) {
            Ok(_) => panic!("unencodable outbound bound was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

        let resolver = storage
            .bind::<0, 1, 1>(seed, egress::Config::default(), &mut driver)
            .expect("validation must precede one-shot transport binding");
        drop(resolver);
    });
    Ok(())
}

use std::num::NonZero;

use dope::{
    core::driver::route::table::conn::Capacity,
    net::{tcp::Tcp, wire::Identity},
};
use dope_fiber::net::{client, read, server};

#[test]
fn reserve_each_connection() {
    let config = read::Config {
        slots: NonZero::<u32>::MIN,
        chunks: NonZero::<u32>::MIN,
        bytes: NonZero::<u32>::MIN,
    };
    assert!(
        client::Plan::<Tcp, Identity>::new(Capacity::fixed::<1>(), Identity)
            .with_receive(config)
            .is_ok()
    );
    let failure = server::Plan::<Identity>::new(Capacity::fixed::<2>(), Identity)
        .with_receive(config)
        .err()
        .expect("one reserved slot per connection");
    assert_eq!(failure.connections, Capacity::fixed::<2>());
    assert_eq!(failure.slots, NonZero::<u32>::MIN);
}

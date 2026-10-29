use std::convert::Infallible;

use dope_core::{
    driver::{life::recovery, route::KeyTag},
    io::socket::install::Install,
};
use dope_test::scope::Error;
use o3::buffer::storage::Zero;

use crate::common::rt::{
    profile,
    profile::{Domain, Minimal, Receive, StreamOnly},
};
pub mod common;

use std::{mem::size_of, net::SocketAddr};

use dope_core::{
    driver::{
        settings,
        settings::{
            Profile,
            claim::{Claim, Needs},
            demands,
            demands::{Accept, Entries, Files, Outbound, slots::Slots},
            errors,
        },
    },
    io::{datagram::payload, transfer},
};
use o3::buffer::bytes::bounded::Overflow;

use crate::common::rt::Runtime;

#[test]
fn accept_files() {
    let claim = Claim::new(settings::Routes::EMPTY)
        .files(Files::accept(Accept::from(Slots::fixed::<112>())));
    let result = Needs::new()
        .include(claim)
        .and_then(|reqs| reqs.resolve::<profile::Files>());
    let config = result.expect("topology resources fit the driver representation");
    assert!((config.file_slots().accept()) == (settings::FileSlots::fixed::<0, 112>().outbound()));
    assert_eq!(config.file_slots().outbound().count().get(), 16);
    assert_eq!(config.file_slots().capacity().count().get(), 128);
}

#[test]
fn receive_layout() {
    let claim = Claim::new(settings::Routes::EMPTY)
        .files(Files::accept(Accept::from(Slots::fixed::<112>())));
    let result = Needs::new()
        .include(claim)
        .and_then(|reqs| reqs.resolve::<Receive>());
    let config = result.expect("topology resources fit the driver representation");
    assert!((config.receive()) == (Receive::RECEIVE));
}

#[test]
fn resident_domains() {
    assert!(settings::Config::default().domains() == settings::Domain::ZERO);
    assert!(settings::Config::for_profile::<Domain>().domains() == settings::Domain::ZERO);
    let claims = (
        Claim::new(settings::Routes::singleton::<KeyTag<1>>())
            .domains(settings::Domain::fixed::<2>()),
        Claim::new(settings::Routes::singleton::<KeyTag<2>>())
            .domains(settings::Domain::fixed::<3>()),
    );
    let result = Needs::new()
        .include(claims.0)
        .and_then(|needs| needs.include(claims.1))
        .and_then(|reqs| reqs.resolve::<Domain>());
    let config = result.expect("resident-domain claims fit the driver representation");
    assert!((config.domains()) == (settings::Domain::fixed::<5>()));

    let claims = (
        Claim::new(settings::Routes::singleton::<KeyTag<3>>()).domains(settings::Domain::MAX),
        Claim::new(settings::Routes::singleton::<KeyTag<4>>()).domains(settings::Domain::ONE),
    );
    assert!(
        matches!(
            Needs::new()
                .include(claims.0)
                .and_then(|needs| needs.include(claims.1))
                .and_then(|needs| needs.resolve::<Domain>()),
            Err(errors::Claim::ResidentDomainsOverflow(_))
        ),
        "resident domain sum must retain both demands"
    );
}

#[test]
fn storage_preserves_value_layout() {
    use std::{alloc::Layout, mem::needs_drop};

    use dope_core::driver::storage::With;

    assert_eq!(Layout::new::<With<(), usize>>(), Layout::new::<usize>());
    assert_eq!(needs_drop::<With<(), usize>>(), needs_drop::<usize>());
}

#[test]
fn composes_demands() {
    let [three, five] = [3_usize, 5].map(|count| {
        Entries::try_from(count).expect("fixture entry counts must fit their representation")
    });

    let claims = (
        Claim::new(settings::Routes::singleton::<KeyTag<1>>())
            .files(Files::new(
                Accept::from(Slots::fixed::<2>()),
                Outbound::fixed::<3>(),
            ))
            .receive(demands::Receive::new(three, transfer::Len::fixed::<64>()))
            .domains(settings::Domain::fixed::<2>())
            .schedule(demands::Schedule::fixed::<3, 5>()),
        Claim::new(settings::Routes::singleton::<KeyTag<2>>())
            .files(Files::new(
                Accept::from(Slots::fixed::<4>()),
                Outbound::fixed::<5>(),
            ))
            .receive(demands::Receive::new(five, transfer::Len::fixed::<32>()))
            .domains(settings::Domain::fixed::<3>())
            .schedule(demands::Schedule::fixed::<7, 11>()),
    );
    let result = Needs::new()
        .include(claims.0)
        .and_then(|needs| needs.include(claims.1))
        .and_then(|needs| needs.exact::<Minimal>());
    let config = result.expect("typed demands compose into one valid layout");

    assert!((config.file_slots().accept()) == (settings::FileSlots::fixed::<0, 6>().outbound()));
    assert!((config.file_slots().outbound()) == (settings::FileSlots::fixed::<0, 8>().outbound()));
    assert!((config.receive()) == (settings::Receive::fixed::<8, 64>()));
    assert!((config.queue_layout()) == (settings::QueueLayout::fixed::<8, 8>()));
    assert!((config.domains()) == (settings::Domain::fixed::<5>()));
    assert_eq!(config.schedule().ready().count().get(), 10);
    assert_eq!(config.schedule().timers().count().get(), 16);
}

#[test]
fn invalid_receive() {
    use settings::Receive;

    let [one, two, three, many] = [1_usize, 2, 3, 1024].map(|count| {
        Entries::try_from(count).expect("fixture entry counts must fit their representation")
    });

    assert!((transfer::Len::try_new(0)) == (Ok(transfer::Len::ZERO)));
    assert!(transfer::Len::try_new(transfer::MAX_BYTES).is_ok());
    assert_eq!(
        size_of::<Result<transfer::Len, Overflow>>(),
        2 * size_of::<usize>(),
    );
    assert!(
        Receive::exact(demands::Receive::new(
            Entries::ZERO,
            transfer::Len::fixed::<4096>()
        )) == Err(errors::Claim::Layout(
            errors::Config::ReceiveEntriesOutOfRange {
                actual: 0,
                minimum: 2,
                maximum: u16::MAX as u32,
            }
        ))
    );
    assert!(
        Receive::exact(demands::Receive::new(one, transfer::Len::fixed::<4096>()))
            == Err(errors::Claim::Layout(
                errors::Config::ReceiveEntriesOutOfRange {
                    actual: 1,
                    minimum: 2,
                    maximum: u16::MAX as u32,
                }
            ))
    );
    assert!(
        Receive::exact(demands::Receive::new(three, transfer::Len::fixed::<4096>()))
            == Ok(Receive::fixed::<4, 4096>())
    );
    assert!(
        Receive::exact(demands::Receive::new(many, transfer::Len::fixed::<0>()))
            == Err(errors::Claim::Layout(errors::Config::ReceiveBuffer(Zero)))
    );
    assert!(matches!(
        Receive::exact(demands::Receive::new(two, transfer::Len::fixed::<1>())),
        Ok(layout) if layout.buffer_len() == transfer::NonZero::fixed::<1>()
    ));
    assert!(matches!(
        Receive::exact(demands::Receive::new(
            two, transfer::Len::MAX,
        )),
        Ok(layout) if layout.buffer_len().into_nonzero().get() as usize == transfer::MAX_BYTES
    ));
    let prefix = Receive::MIN_DATAGRAM_SLOT.into_nonzero().get() - 1;
    assert!(
        payload::Len::try_from(transfer::MAX_BYTES as u32)
            == Err(payload::Error::TooLarge {
                payload: transfer::MAX_BYTES as u32,
                prefix,
                actual: u64::from(prefix) + transfer::MAX_BYTES as u64,
                maximum: transfer::MAX_BYTES as u32,
            })
    );
    assert!(
        settings::Ring::try_from(1)
            == Err(errors::Config::ReceiveEntriesOutOfRange {
                actual: 1,
                minimum: 2,
                maximum: u16::MAX as u32,
            })
    );
    assert!(
        settings::Ring::try_from(3)
            == Err(errors::Config::ReceiveEntriesNotPowerOfTwo { actual: 3 })
    );
    assert!(
        payload::Len::try_from(0)
            == Err(payload::Error::Empty {
                actual: 0,
                minimum: 1,
            })
    );
    let udp = Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    assert!(
        (udp.receive().buffer_len())
            == (transfer::NonZero::fixed::<{ Receive::MIN_DATAGRAM_SLOT.into_nonzero().get() + 7 }>(
            ))
    );
}

#[test]
fn datagram_layout() {
    use std::num::NonZero;

    for (demand, slots) in [(2, 2), (3, 4), (16_385, 32_768), (32_768, 32_768)] {
        let result = settings::Ring::try_round(demand);
        let ring = result.expect("representable receive demand must round up");
        assert_eq!(ring.buffers().into_nonzero().get(), slots);
        for bytes in [NonZero::<u16>::MIN, NonZero::<u16>::MAX] {
            let payload = payload::Len::from(bytes);
            assert!(payload::Len::try_from(u32::from(bytes.get())) == Ok(payload));
            let len = settings::Receive::MIN_DATAGRAM_SLOT.into_nonzero().get()
                + u32::from(bytes.get())
                - 1;
            let total = u64::from(slots) * u64::from(len);
            if total > isize::MAX as u64 {
                assert!(
                    settings::Datagram::try_new(ring, payload)
                        == Err(errors::Config::ReceiveAllocationTooLarge {
                            entries: slots,
                            buffer_len: len,
                            actual: total,
                            maximum: isize::MAX as u64,
                        },)
                );
                continue;
            }
            let datagram = (settings::Datagram::try_new(ring, payload))
                .expect("bounded datagram layout must fit the native allocation");
            let receive = datagram.receive();
            let claim = demands::Receive::from(datagram);
            assert!(settings::Receive::exact(claim) == Ok(receive));
            assert_eq!(receive.buffer_len().into_nonzero().get(), len,);
        }
    }
    for actual in [0, 1] {
        assert!(
            settings::Ring::try_round(actual)
                == Err(errors::Claim::Layout(
                    errors::Config::ReceiveEntriesOutOfRange {
                        actual,
                        minimum: 2,
                        maximum: u16::MAX as u32,
                    },
                ))
        );
    }
    assert!(
        settings::Ring::try_round(32_769)
            == Err(errors::Claim::ReceiveEntriesTooLarge {
                actual: 65_536,
                maximum: u16::MAX as usize,
            },)
    );
    assert!(
        settings::Ring::try_round(u32::MAX)
            == Err(errors::Claim::ReceiveEntriesPowerOfTwoOverflow {
                requested: u32::MAX
            },)
    );
}

#[test]
fn stream_datagram() {
    use std::net::TcpListener;

    use dope_core::io::socket::install;

    let mut reports = Vec::new();

    Runtime::for_profile::<StreamOnly>().with_driver(
        |mut driver| {
            let address = SocketAddr::from(([127, 0, 0, 1], 0));
            let first = (TcpListener::bind(address)).expect("first adoption input");
            for result in [
                Install::bind_udp(driver.reborrow(), address, common::DATAGRAM),
                Install::adopt_udp(driver.reborrow(), first.into()),
            ] {
                assert!(
                    matches!(
                        result,
                        Err(install::Error::Receive(error))
                            if error.actual() == transfer::NonZero::fixed::<1>()
                                && error.required() == settings::Receive::MIN_DATAGRAM_SLOT
                    ),
                    "receive capacity must fail before socket validation"
                );
            }
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn failed_merge_is_atomic() {
    let claims = Needs::new()
        .include(
            Claim::new(settings::Routes::singleton::<KeyTag<1>>())
                .schedule(demands::Schedule::fixed::<1, 0>()),
        )
        .expect("initial claim");

    let result = claims.include(
        Claim::new(settings::Routes::singleton::<KeyTag<2>>())
            .files(Files::accept(Accept::from(Slots::fixed::<1>())))
            .schedule(demands::Schedule::new(Slots::MAX, Slots::ZERO)),
    );
    let error = (result).err().expect("late scheduler overflow");
    let sum = Slots::fixed::<1>()
        .checked_add(Slots::MAX)
        .err()
        .map(errors::Claim::SchedulerReadyOverflow);
    assert!(sum == Some(error));

    assert!(
        claims
            .include(
                Claim::new(settings::Routes::singleton::<KeyTag<2>>())
                    .files(Files::accept(Accept::from(Slots::fixed::<{ u32::MAX }>()),),),
            )
            .is_ok(),
        "failed merge must preserve neither route nor descriptors"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}

fn finish(result: Result<(), Error<'_, ()>>) {
    result
        .as_ref()
        .map_err(Error::cause)
        .expect("driver lifecycle");
}

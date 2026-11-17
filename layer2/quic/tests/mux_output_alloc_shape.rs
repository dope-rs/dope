use dope_quic::{errors, runtime::mux::builder};
use dope_test::alloc::Tracker;
use std::{pin::Pin, time::Duration};

pub mod common;
use common::suite::mux::{dispatch::Capture, output::Seen};

#[test]
fn small_burst_stays_plain() {
    const CONNECTIONS: usize = 3;
    let (mut client, handles, now) =
        Seen::try_many(true, CONNECTIONS).expect("GSO fixture must establish");
    for &handle in &handles {
        Seen::enqueue(&mut client, handle, 1, 1_150, now).expect("output enqueue");
    }

    let burst = Pin::new(&mut client)
        .observe(now, |mut turn| {
            turn.output().drive()?;
            Ok::<Vec<_>, errors::Transmit>(turn.output().drain().collect())
        })
        .expect("fixture connections must admit the small burst");
    assert_eq!(burst.len(), CONNECTIONS);
    assert_eq!(Seen::packet_count(&burst), CONNECTIONS);
    assert!(burst.iter().all(|outgoing| !outgoing.is_segmented()));
}

#[test]
fn wire_shapes_stay_plain() {
    const CONNECTIONS: usize = 4;
    let (mut client, handles, now) =
        Seen::try_many(true, CONNECTIONS).expect("GSO fixture must establish");
    for (ordinal, &handle) in handles.iter().enumerate() {
        Seen::enqueue(&mut client, handle, 1, 1_100 + ordinal, now).expect("output enqueue");
    }

    let burst = Pin::new(&mut client)
        .observe(now, |mut turn| {
            turn.output().drive()?;
            Ok::<Vec<_>, errors::Transmit>(turn.output().drain().collect())
        })
        .expect("fixture connections must admit the varied wire shapes");
    assert_eq!(burst.len(), CONNECTIONS);
    assert_eq!(Seen::packet_count(&burst), CONNECTIONS);
    assert!(burst.iter().all(|outgoing| !outgoing.is_segmented()));
}

#[test]
fn reuses_owner() {
    for (gso, packets) in [(false, 1), (true, 4)] {
        let (mut client, handles, mut now) = Seen::try_many(gso, 2).expect("connected peers");
        let bytes = if gso { 1_150 } else { 64 };
        let mut flights = Vec::with_capacity(handles.len());
        for &handle in &handles {
            Seen::enqueue(&mut client, handle, packets, bytes, now).expect("output enqueue");
            flights.push(
                Seen::take_one(&mut client, now)
                    .expect("output take_one")
                    .expect("warm flight"),
            );
        }
        let owners = [flights[0].payload().as_ptr(), flights[1].payload().as_ptr()];
        for flight in flights.drain(..) {
            assert_eq!(flight.is_segmented(), gso);
            Pin::new(&mut client).output().recycle(flight);
        }

        now += Duration::from_millis(1);
        for &handle in &handles {
            Seen::enqueue(&mut client, handle, packets, bytes, now).expect("output enqueue");
        }
        let span = Tracker::begin();
        for _ in &handles {
            flights.push(
                Seen::take_one(&mut client, now)
                    .expect("output take_one")
                    .expect("reused flight"),
            );
        }
        let activity = span.finish();

        assert_eq!(activity.allocations, 0);
        for flight in flights {
            assert_eq!(flight.is_segmented(), gso);
            assert!(owners.contains(&flight.payload().as_ptr()));
            Pin::new(&mut client).output().recycle(flight);
        }
    }
}

#[test]
fn single_slot_gso_reuses_native_owner() {
    let (mut client, handles, mut now) = Seen::try_many(false, 1).expect("native peer");
    let handle = handles[0];
    let capacity = Pin::new(&mut client).output().capacity();
    Seen::enqueue(&mut client, handle, capacity, 64, now).expect("fill output");
    let packet = Seen::take_one(&mut client, now)
        .expect("native output")
        .expect("one reusable packet");
    let owner = packet.payload().as_ptr();
    Pin::new(&mut client).output().recycle(packet);
    Pin::new(&mut client)
        .config()
        .enable_gso()
        .expect("GSO admission");

    now += Duration::from_millis(1);
    Seen::enqueue(&mut client, handle, 4, 1_150, now).expect("GSO-ready payloads");
    let span = Tracker::begin();
    Pin::new(&mut client)
        .observe(now, |mut turn| turn.output().drive())
        .expect("single slot output");
    assert_eq!(span.finish().allocations, 0);
    assert_eq!(Pin::new(&mut client).output().len(), capacity);
    let packet = Pin::new(&mut client)
        .observe(now, |mut turn| turn.output().drain().last())
        .expect("single slot packet");
    assert_eq!(packet.packet_count().get(), 1);
    assert_eq!(packet.payload().as_ptr(), owner);
}

#[test]
fn native_batch_returns_credit_and_storage() {
    use common::suite::mux::output::SERVER;
    use dope_quic::conn::output::transmit::{Admission, Error};

    const PACKETS: usize = 8;
    let (mut client, handles, mut now) = Seen::try_many(false, 1).expect("native peer");
    let handle = handles[0];
    Seen::enqueue(&mut client, handle, PACKETS, 64, now).expect("warm packets");
    Pin::new(&mut client).observe(now, |mut turn| {
        turn.output().drive().expect("warm native output");
        for flight in turn.output().drain() {
            drop(flight);
        }
    });
    now += Duration::from_millis(1);
    Seen::enqueue(&mut client, handle, PACKETS, 64, now).expect("pending packets");
    let failure = Tracker::fail_after(1, || {
        Pin::new(&mut client).observe(now, |mut turn| turn.output().drive())
    })
    .expect_err("the second packet allocation fails");
    assert!(matches!(
        failure.into_parts().1,
        Error::Admission(Admission::Payload {
            error: o3::alloc::Error::Exhausted,
            ..
        })
    ));
    assert_eq!(Pin::new(&mut client).output().len(), 1);
    assert!(Pin::new(&mut client).output().bytes() != 0);
    let warm: Vec<_> = Pin::new(&mut client).observe(now, |mut turn| {
        turn.output().drive().expect("retry owns every packet");
        turn.output().drain().collect()
    });
    assert_eq!(warm.len(), PACKETS);
    let owners: Vec<_> = warm
        .iter()
        .map(|flight| flight.payload().as_ptr())
        .collect();
    for flight in warm {
        Pin::new(&mut client).output().recycle(flight);
    }
    now += Duration::from_millis(1);
    Seen::enqueue(&mut client, handle, PACKETS, 64, now).expect("reuse packets");
    let span = Tracker::begin();
    Pin::new(&mut client)
        .observe(now, |mut turn| turn.output().drive())
        .expect("reused native output");
    assert_eq!(span.finish().allocations, 0);
    let measured: Vec<_> =
        Pin::new(&mut client).observe(now, |mut turn| turn.output().drain().collect());
    assert_eq!(measured.len(), PACKETS);
    for (flight, owner) in measured.into_iter().zip(owners.into_iter().rev()) {
        assert_eq!(flight.addr(), SERVER);
        assert_eq!(flight.packet_count().get(), 1);
        assert_eq!(flight.payload().as_ptr(), owner);
        Pin::new(&mut client).output().recycle(flight);
    }
}

#[test]
fn gso_reuses_across_conns() {
    const CONNECTIONS: usize = 4;
    let (mut client, handles, mut now) =
        Seen::try_many(true, CONNECTIONS).expect("output fixture must establish");

    for &handle in &handles {
        Seen::enqueue(&mut client, handle, 1, 1_150, now).expect("output enqueue");
    }
    let warm = Seen::take_one(&mut client, now)
        .expect("output take_one")
        .expect("the enqueued payload must produce one mux flight");
    assert!(warm.is_segmented());
    assert_eq!(warm.packet_count().get(), CONNECTIONS);
    let owner = warm.payload().as_ptr();
    Pin::new(&mut client).output().recycle(warm);

    now += Duration::from_millis(1);
    for &handle in &handles {
        Seen::enqueue(&mut client, handle, 1, 1_150, now).expect("output enqueue");
    }
    let span = Tracker::begin();
    let measured = Seen::take_one(&mut client, now)
        .expect("output take_one")
        .expect("the enqueued payload must produce one mux flight");
    let activity = span.finish();

    assert_eq!(activity.allocations, 0);
    assert!(measured.is_segmented());
    assert_eq!(measured.packet_count().get(), CONNECTIONS);
    assert_eq!(measured.payload().as_ptr(), owner);
    Pin::new(&mut client).output().recycle(measured);
}

#[test]
fn allocation_returns() {
    let limits = builder::Plan::try_new(1, 2, 2_400).expect("valid output limits");
    let capture = Capture::default();
    let builder = builder::Builder::client(capture.clone()).limits(limits);
    let (result, count) = Tracker::count(|| builder.build());
    drop(result.expect("output allocation baseline"));

    let mut output = false;
    for budget in 0..count {
        let builder = builder::Builder::client(capture.clone()).limits(limits);
        let (result, activity) =
            Tracker::activity(|| Tracker::fail_after(budget, || builder.build()));
        let error = result.err().expect("failed allocation returns its cause");
        if let builder::Failure::Output {
            packets,
            bytes,
            error,
        } = error
        {
            assert_eq!(error, o3::alloc::Error::Exhausted);
            assert_eq!(packets.get(), limits.outgoing());
            assert_eq!(bytes, limits.bytes());
            output = true;
        }
        assert_eq!(activity.allocations, activity.deallocations);
        assert_eq!(activity.allocated_bytes, activity.deallocated_bytes);
    }
    assert!(
        output,
        "output storage must exercise its allocation failures"
    );
    assert!(
        capture
            .events
            .borrow()
            .owners
            .iter()
            .all(|(owner, handle)| owner == handle)
    );
}

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

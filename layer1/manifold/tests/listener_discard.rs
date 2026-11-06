pub mod common;

use common::manifold::listener::{native, reached};

use std::{
    cell::Cell,
    marker,
    net::Shutdown,
    rc::Rc,
    time::{Duration, Instant},
};

use common::{
    manifold::listener::pipeline::discard::{
        ACK, Discard, Event, Finish, PAYLOAD, PREFIX, REQUEST,
    },
    recover,
    scenario::case::listener::Listener,
};
use dope_core::driver::{
    life::recovery,
    route::{FrameworkTag, KeyTag, Tag, kind},
};
use dope_manifold::{
    app::{env::Bundle, receive},
    network::listener::handler::{self, Application},
    timing::Throughput,
};
use dope_net::{tcp::Tcp, wire::Identity};
use dope_runtime::executor::session::Completion;
use dope_test::{
    harness::{
        GUARD,
        peer::{self, Peer},
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

fn observe(event: Event<'_, '_>) {
    use dope_manifold::network::listener::conn::write;
    use dope_net::wire::receive::discard;
    assert!(match event {
        Event::Request(bytes) => bytes == REQUEST,
        Event::Payload(bytes) => PAYLOAD.starts_with(bytes),
        Event::Length(len) => len == PAYLOAD.len(),
        Event::Discard(start) => matches!(start, discard::Start::Started),
        Event::Discarded => true,
        Event::Deadline(armed) => armed.is_ok(),
        Event::Write(result) => matches!(result, Ok(write::Outcome::Committed(()))),
    });
}

#[test]
fn borrowed_keeps_payload() {
    run::<receive::Borrowed, _>(observe);
}

#[test]
fn retained_keeps_payload() {
    run::<receive::Owned, _>(observe);
}

fn run<I: receive::Mode<Identity>, F: for<'a, 'b> Fn(Event<'a, 'b>)>(observe: F)
where
    for<'d> Discard<I, F>: Application<'d, KeyTag<0>, Wire = Identity, Input = I>
        + handler::Runtime<'d, 'd, KeyTag<0>>,
{
    let mut finish = Completion::new();
    let gate = Gate::new();
    let retired = Gate::new();
    let prepared = Listener::<I>::new::<64>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            Discard {
                gate: gate.clone(),
                retired: retired.clone(),
                finish: Finish::Payload,
                input: marker::PhantomData,
                observe,
            },
            Identity,
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                let mut request = PREFIX.to_vec();
                request.extend_from_slice(PAYLOAD);
                Peer::at(case.addr())
                    .exchange(request, |peer| {
                        reached(case.try_wait(&retired, 1), retired.hits(), 1);
                        assert_eq!(
                            peer.finish()
                                .expect("finish client peer")
                                .expect("client peer failed"),
                            PAYLOAD
                        );
                        assert_eq!(gate.hits(), 1);
                    })
                    .expect("spawn payload peer");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn partial_prefix_finishes() {
    let expired = Gate::new();
    let close_at = Rc::new(Cell::new(None));
    for finish in [
        Finish::Prefix(close_at.clone()),
        Finish::Eof(close_at.clone()),
        Finish::Timeout(expired.clone()),
    ] {
        close_at.set(None);
        let complete = matches!(finish, Finish::Prefix(_));
        let eof = matches!(finish, Finish::Eof(_));
        let timeout = matches!(finish, Finish::Timeout(_));
        let discarded = Cell::new(0);
        let mut done = Completion::new();
        let closed = Gate::new();
        let retired = Gate::new();
        let prepared = Listener::<receive::Coupled>::new::<4>(Default::default())
            .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
                Discard {
                    gate: closed.clone(),
                    retired: retired.clone(),
                    finish,
                    input: marker::PhantomData,
                    observe: |event: Event<'_, '_>| match event {
                        Event::Discarded => discarded.set(discarded.get() + 1),
                        event => observe(event),
                    },
                },
                Identity,
            ))
            .expect("listener layout");
        let mut reports =
            Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
        prepared
            .with_run(
                &mut done,
                |case| {
                    Peer::at(case.addr())
                        .with(
                            move |socket| -> Result<(Option<Instant>, Vec<u8>), peer::Error> {
                                socket.write_all(REQUEST)?;
                                let mut ack = [0; ACK.len()];
                                socket.read_exact(&mut ack)?;
                                assert_eq!(ack, ACK);
                                if !timeout {
                                    socket.write_all(&PREFIX[..PREFIX.len() - 1])?;
                                    socket.read_wait(Some(Duration::from_millis(20)))?;
                                    let error = socket.read(&mut [0]).expect_err(
                                        "incomplete prefix must keep the connection open",
                                    );
                                    assert!(matches!(error, peer::Error::TimedOut)
                                        || matches!(error, peer::Error::Syscall(error) if error.is_block()));
                                    socket.read_wait(Some(GUARD))?;
                                }
                                let released = (!timeout).then(Instant::now);
                                if complete {
                                    socket.write_all(&PREFIX[PREFIX.len() - 1..])?;
                                } else if eof {
                                    socket.shutdown(Shutdown::Write)?;
                                }
                                let mut trailing = Vec::new();
                                socket.read_to_end(&mut trailing)?;
                                Ok((released, trailing))
                            },
                            |peer| {
                                reached(case.try_wait(&retired, 1), retired.hits(), 1);
                                let (released, trailing) = peer
                                    .finish()
                                    .expect("finish prefix peer")
                                    .expect("prefix peer");
                                assert!(trailing.is_empty());
                                if let Some(released) = released {
                                    assert!(
                                        close_at.get().expect("close timestamp") >= released,
                                        "incomplete prefix closed early"
                                    );
                                }
                                assert_eq!(closed.hits(), 1);
                                assert_eq!(expired.hits(), u32::from(timeout));
                                assert_eq!(discarded.get(), u32::from(complete));
                            },
                        )
                        .expect("spawn prefix peer");
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                native,
            )
            .expect("listener run");
        assert!(
            reports.events().expect("reclaimed events").is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );
    }
}

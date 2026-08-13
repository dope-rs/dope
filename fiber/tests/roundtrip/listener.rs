use std::{
    io::{Read, Write},
    time::Duration,
};

use dope::{
    manifold::timing::Balanced,
    net::{
        tcp::Tcp,
        wire::{Cursor as _, Identity},
    },
};
use dope_fiber::net::{read::Lease, server::Listener};

const MAX_CONN: usize = 8;

type Pool<'scope, 'd> = Listener<'scope, 'd, 0, Tcp, Identity>;

fn append_up_to(lease: &mut Lease<'_, '_, Identity>, output: &mut Vec<u8>, limit: usize) {
    while output.len() < limit && !lease.is_empty() {
        let chunk = lease.chunk();
        let amount = chunk.len().min(limit - output.len());
        output.extend_from_slice(&chunk[..amount]);
        assert_eq!(lease.consume(amount), amount);
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct App<'d, 'scope> {
    #[pin]
    #[manifold]
    pool: Pool<'scope, 'd>,
    #[dispatcher(marker)]
    driver: ::core::marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

#[test]
fn fiber_listener_roundtrip() {
    let addr = dope_test::peer::Peer::reserve().addr();
    dope_test::scenario::rt::Runtime::tcp_listener::<Balanced>(MAX_CONN, |c| c)
        .try_enter(|mut sess| {
            let hash_builder = sess.hash_state(dope::manifold::listener::Domain::DEFAULT);
            let storage = sess.storage();
            let mut driver = sess.driver_access();
            let listener = Pool::bind(
                storage,
                &mut driver,
                addr,
                16,
                Default::default(),
                Default::default(),
                hash_builder,
            )
            .expect("bind");
            let pool = sess.storage().handle();
            let client = dope_test::peer::Peer::at(addr).spawn(|stream| {
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("timeout");
                stream.write_all(b"abcdefg").expect("write first request");
                let mut ack = [0; 1];
                stream.read_exact(&mut ack).expect("read acknowledgement");
                stream.write_all(b"xyz").expect("write second request");
                let mut reply = [0; 4];
                stream.read_exact(&mut reply).expect("read");
                (ack, reply)
            });
            let dispatcher = App {
                pool: listener,
                driver: ::core::marker::PhantomData,
            };
            let (prefix, tail, reused) = sess
                .with_app(dispatcher, |mut app| {
                    let stream = dope_test::fibers::TEST
                        .drive(
                            &mut app,
                            dope_gen::fiber!('_, crate = ::dope_fiber => async move { pool.accept().await }),
                        )
                        .expect("accept");
                    let mut stream = Some(stream);
                    dope_test::fibers::TEST
                        .drive(
                            &mut app,
                            dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                                let mut stream = stream.take().expect("stream owner");
                                let mut retained = None;
                                let mut prefix = Vec::with_capacity(5);
                                while prefix.len() < 5 {
                                    if retained.as_ref().is_none_or(Lease::is_empty) {
                                        drop(retained.take());
                                        retained = stream.read().await?;
                                    }
                                    let lease = retained.as_mut().ok_or_else(|| {
                                        std::io::Error::from(std::io::ErrorKind::UnexpectedEof)
                                    })?;
                                    append_up_to(lease, &mut prefix, 5);
                                }
                                let mut tail = Vec::with_capacity(2);
                                while tail.len() < 2 {
                                    if retained.as_ref().is_none_or(Lease::is_empty) {
                                        drop(retained.take());
                                        retained = stream.read().await?;
                                    }
                                    let lease = retained.as_mut().ok_or_else(|| {
                                        std::io::Error::from(std::io::ErrorKind::UnexpectedEof)
                                    })?;
                                    append_up_to(lease, &mut tail, 2);
                                }
                                drop(retained);
                                dope_test::fibers::TEST.cancel_after_poll(
                                    stream.write_all(b"discarded"),
                                )
                                .await;
                                stream.write_all(b"!").await?;
                                let mut retained = None;
                                let mut reused = Vec::with_capacity(3);
                                while reused.len() < 3 {
                                    if retained.as_ref().is_none_or(Lease::is_empty) {
                                        drop(retained.take());
                                        retained = stream.read().await?;
                                    }
                                    let lease = retained.as_mut().ok_or_else(|| {
                                        std::io::Error::from(std::io::ErrorKind::UnexpectedEof)
                                    })?;
                                    append_up_to(lease, &mut reused, 3);
                                }
                                drop(retained);
                                stream.write_all(b"pong").await?;
                                Ok::<_, std::io::Error>((prefix, tail, reused))
                            }),
                        )
                        .expect("roundtrip")
                })
                .expect("application teardown");
            assert_eq!(prefix, b"abcde");
            assert_eq!(tail, b"fg");
            assert_eq!(reused, b"xyz");
            assert_eq!(client.join().expect("join"), (*b"!", *b"pong"));
        })
        .expect("listener port storage");
}

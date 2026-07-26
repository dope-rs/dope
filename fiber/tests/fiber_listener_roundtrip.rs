use dope_test as common;

use std::io::{Read, Write};
use std::pin::pin;
use std::time::Duration;

extern crate dope;
use dope::runtime::profile::Balanced;
use dope_fiber::net::listener::Listener;
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;
use o3::cell::BrandCell;

const MAX_CONN: usize = 8;

type Pool<'scope, 'd> = Listener<'scope, 'd, 0, Tcp, Identity>;

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct App<'d, 'scope> {
    #[pin]
    #[manifold]
    pool: Pool<'scope, 'd>,
}

#[test]
fn fiber_listener_roundtrip() {
    let addr = common::reserve_addr();
    common::listener_exec::<Balanced>(MAX_CONN, |c| c).enter(|mut sess| {
        let hash_builder = sess.seed().derive(dope::hash::domain::ACCEPT).state();
        let (storage, mut driver) = sess.storage_and_driver();
        let pool = Pool::bind(
            storage.get_ref(),
            &mut driver,
            &addr,
            16,
            Default::default(),
            Default::default(),
            hash_builder,
        )
        .expect("bind");
        let app = pin!(BrandCell::new(App { pool }));
        let pool = sess.storage().handle();
        let client = common::spawn_peer(addr, |stream| {
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
        let stream = common::drive(
            &mut sess,
            app.as_ref(),
            dope_gen::fiber!('_ => async move { pool.accept().await }),
        )
        .expect("accept");
        let mut stream = Some(stream);
        let (prefix, tail, reused) = common::drive(
            &mut sess,
            app.as_ref(),
            dope_gen::fiber!('_ => async move {
                let mut stream = stream.take().expect("stream owner");
                let mut prefix = Vec::with_capacity(5);
                while prefix.len() < 5 {
                    let (read, buf) = stream.read(vec![0; 5 - prefix.len()]).await;
                    let read = read?;
                    if read == 0 {
                        return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
                    }
                    prefix.extend_from_slice(&buf[..read]);
                }
                let mut tail = Vec::with_capacity(2);
                while tail.len() < 2 {
                    let (read, buf) = stream.read(vec![0; 2 - tail.len()]).await;
                    let read = read?;
                    if read == 0 {
                        return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
                    }
                    tail.extend_from_slice(&buf[..read]);
                }
                stream.write_all(b"!").await?;
                let mut reused = Vec::with_capacity(3);
                while reused.len() < 3 {
                    let (read, buf) = stream.read(vec![0; 3 - reused.len()]).await;
                    let read = read?;
                    if read == 0 {
                        return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
                    }
                    reused.extend_from_slice(&buf[..read]);
                }
                stream.write_all(b"pong").await?;
                Ok::<_, std::io::Error>((prefix, tail, reused))
            }),
        )
        .expect("roundtrip");
        assert_eq!(prefix, b"abcde");
        assert_eq!(tail, b"fg");
        assert_eq!(reused, b"xyz");
        assert_eq!(client.join().expect("join"), (*b"!", *b"pong"));
    });
}

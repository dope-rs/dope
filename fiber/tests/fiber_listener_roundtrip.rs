use dope_test as common;

use std::io::{Read, Write};
use std::pin::pin;
use std::time::Duration;

extern crate dope;
use dope::runtime::profile::Balanced;
use dope_fiber::Listener;
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
                let mut prefix = [0; 5];
                let mut filled = 0;
                while filled < prefix.len() {
                    let read = stream.read_into(&mut prefix[filled..]).await?;
                    if read == 0 {
                        return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
                    }
                    filled += read;
                }
                let tail = stream
                    .read_chunk()
                    .await?
                    .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?;
                let tail = tail.as_slice().to_vec();
                stream.write_all_static(b"!").await?;
                let mut reused = [0; 3];
                let mut filled = 0;
                while filled < reused.len() {
                    let read = stream.read_into(&mut reused[filled..]).await?;
                    if read == 0 {
                        return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
                    }
                    filled += read;
                }
                stream.write_all_static(b"pong").await?;
                Ok::<_, std::io::Error>((prefix, tail, reused))
            }),
        )
        .expect("roundtrip");
        assert_eq!(prefix, *b"abcde");
        assert_eq!(tail, b"fg");
        assert_eq!(reused, *b"xyz");
        assert_eq!(client.join().expect("join"), (*b"!", *b"pong"));
    });
}

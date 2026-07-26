use std::io::{Read, Write};
use std::pin::pin;
use std::time::Duration;

extern crate dope;
use dope::runtime::profile::Balanced;
use dope_fiber::net::connector::ConnectorPort;
use dope_fiber::net::listener::{Listener, ListenerPort};
use dope_net::tcp::Tcp;
use dope_net::wire::send::{Plain, Prepared, Storage, Vectored};
use dope_net::wire::{ReadyOpen, Reclaim, RuntimeLimits, Wire};
use o3::buffer::{Bytes, Leased, SharedPool};
use o3::cell::BrandCell;

use dope_test as common;

const MAX_CONN: usize = 2;
const QUEUE_CAP: usize = 256;

type Pool<'scope, 'd> = Listener<'scope, 'd, 0, Tcp, PooledWire>;

struct PooledWire;

#[test]
fn receive_capacity_is_validated_before_storage_build() {
    let listener = ListenerPort::factory(0)
        .err()
        .expect("zero listener capacity");
    assert_eq!(listener.kind(), std::io::ErrorKind::InvalidInput);
    let connector = ConnectorPort::<Tcp>::factory(usize::MAX)
        .err()
        .expect("overflowing connector capacity");
    assert_eq!(connector.kind(), std::io::ErrorKind::InvalidInput);
}

impl Wire for PooledWire {
    type InitConfig = ();
    type RuntimeContext = SharedPool;
    type Open<'a> = ReadyOpen<Self>;
    type Recv<'a> = Bytes<Leased>;
    type SendStorage = ();

    const RECLAIM: Reclaim = Reclaim::OnComplete;

    fn runtime_context(
        limits: RuntimeLimits,
        _: Self::InitConfig,
    ) -> std::io::Result<Self::RuntimeContext> {
        assert!(limits.max_retained_recv_chunks() >= QUEUE_CAP);
        SharedPool::try_new(limits.max_retained_recv_chunks() + 1, limits.max_recv_len())
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
    }

    fn prepare_open(_: &mut Self::RuntimeContext) -> Option<Self::Open<'_>> {
        Some(ReadyOpen::new(Self, ()))
    }

    fn process_recv<'a>(
        &mut self,
        runtime: &mut Self::RuntimeContext,
        bytes: &'a [u8],
    ) -> Option<Self::Recv<'a>> {
        let mut lease = runtime.try_acquire()?;
        lease.spare_writer().try_extend_from_slice(bytes).ok()?;
        Some(Bytes::<Leased>::from(lease.freeze()))
    }

    fn prepare_send<'a>(
        &'a mut self,
        _: Storage<'a, Self::SendStorage>,
        plain: Plain<'a>,
    ) -> Prepared<'a> {
        let len = plain.len();
        Prepared::input(plain, len)
    }

    fn prepare_send_vectored<'a>(
        &'a mut self,
        _: Storage<'a, Self::SendStorage>,
        plain: Vectored<'a>,
    ) -> Prepared<'a> {
        let len = plain.bytes();
        Prepared::vectored(plain, len)
    }

    fn after_send<'a>(
        &'a mut self,
        send: Storage<'a, Self::SendStorage>,
        _: usize,
    ) -> Prepared<'a> {
        send.empty(0)
    }

    fn flush_pending<'a>(&'a mut self, send: Storage<'a, Self::SendStorage>) -> Prepared<'a> {
        send.empty(0)
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct App<'d, 'scope> {
    #[pin]
    #[manifold]
    pool: Pool<'scope, 'd>,
}

#[test]
fn receive_hoarder_cannot_starve_another_connection() {
    let addr = common::reserve_addr();
    let exec = common::listener_exec::<Balanced>(MAX_CONN, |c| c.with_provided(1, 1024));
    exec.enter(|mut sess| {
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
        let clients = std::thread::spawn(move || {
            let mut hoarder = common::connect_with_read_timeout(addr, Duration::from_secs(3));
            let mut probe = common::connect_with_read_timeout(addr, Duration::from_secs(3));
            std::thread::sleep(Duration::from_millis(250));
            hoarder
                .write_all(&vec![b'H'; QUEUE_CAP])
                .expect("fill receive queue");
            hoarder.flush().expect("flush receive queue");
            std::thread::sleep(Duration::from_millis(500));

            probe.write_all(b"P").expect("write probe");
            let mut reply = [0; 1];
            probe.read_exact(&mut reply).expect("read probe reply");
            drop(hoarder);
            reply
        });
        let hoarder = common::drive(
            &mut sess,
            app.as_ref(),
            dope_gen::fiber!('_ => async move { pool.accept().await }),
        )
        .expect("accept hoarder");
        let probe = common::drive(
            &mut sess,
            app.as_ref(),
            dope_gen::fiber!('_ => async move { pool.accept().await }),
        )
        .expect("accept probe");
        let mut probe = Some(probe);
        let (probe, result) = common::drive(
            &mut sess,
            app.as_ref(),
            dope_gen::fiber!('_ => async move {
                let mut probe = probe.take().expect("probe owner");
                let (read, request) = probe.read(vec![0; 1]).await;
                if read? == 0 {
                    return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
                }
                Ok::<_, std::io::Error>((probe, request))
            }),
        )
        .expect("read probe");
        let mut probe = Some(probe);
        common::drive(
            &mut sess,
            app.as_ref(),
            dope_gen::fiber!('_ => async move {
                let mut probe = probe.take().expect("probe owner");
                probe.write_all(b"R").await?;
                Ok::<_, std::io::Error>(probe)
            }),
        )
        .expect("reply probe");
        drop(hoarder);
        assert_eq!(result, [b'P']);
        assert_eq!(clients.join().expect("clients join"), [b'R']);
    });
}

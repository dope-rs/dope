use std::{
    convert::Infallible,
    io::{Read, Write},
    time::Duration,
};

use dope::{
    core::{
        driver::{route::SLOT_MASK, settings},
        io::recv::Lease,
    },
    manifold::timing::Balanced,
    net::{
        tcp::Tcp,
        wire::{
            self, Cursor as _, ReadyOpen, RecvChunk, RuntimeLimits, Wire, reclaim,
            send::{Plain, Prepared, Sent, Storage, Transition, Vectored},
        },
    },
};
use dope_fiber::net::{
    connector::Port,
    server::{Listener, ListenerPort},
};
use o3::buffer::{
    self,
    bytes::{Bytes, Retained},
};

const MAX_CONN: usize = 2;
const QUEUE_CAP: usize = 256;

type Pool<'scope, 'd> = Listener<'scope, 'd, 0, Tcp, PooledWire>;

struct PooledWire;

#[test]
fn receive_capacity_is_validated_before_storage_build() {
    let maximum = SLOT_MASK as usize;
    assert!(ListenerPort::<PooledWire>::factory(maximum).is_ok());
    assert!(Port::<Tcp, PooledWire>::factory(maximum).is_ok());

    for invalid in [0, maximum + 1, usize::MAX] {
        let listener = ListenerPort::<PooledWire>::factory(invalid)
            .err()
            .expect("invalid listener capacity");
        assert_eq!(listener.kind(), std::io::ErrorKind::InvalidInput);
        let connector = Port::<Tcp, PooledWire>::factory(invalid)
            .err()
            .expect("invalid connector capacity");
        assert_eq!(connector.kind(), std::io::ErrorKind::InvalidInput);
    }
}

impl Wire for PooledWire {
    type Connection<'d, const ID: u8> = Self;
    type ConnectionStorage<const ID: u8> = ();
    type InitConfig<'d, const ID: u8> = ();
    type RuntimeContext<'d, const ID: u8> = buffer::Pool;
    type Open<'a, 'd, const ID: u8>
        = ReadyOpen<Self::Connection<'d, ID>, Self::StorageBackend<'d>>
    where
        'd: 'a;
    type OpenError = Infallible;
    type Recv<'a> = Bytes<Retained>;
    type RecvBatch<'a> = std::option::IntoIter<RecvChunk<'a, Self::Recv<'a>>>;
    type RetainedRecv<'d> = Bytes<Retained>;
    type StorageBackend<'d>
        = ()
    where
        Self: 'd;
    type Reclaim = reclaim::OnComplete;
    type Receive = wire::receive::Direct;

    fn connection_storage<const ID: u8>(connections: usize) -> std::io::Result<()> {
        assert!((1..=SLOT_MASK as usize).contains(&connections));
        Ok(())
    }

    fn runtime_context<'d, const ID: u8>(
        limits: RuntimeLimits,
        _: Self::InitConfig<'d, ID>,
    ) -> std::io::Result<Self::RuntimeContext<'d, ID>>
    where
        Self: 'd,
    {
        assert!(limits.max_retained_recv_chunks() >= QUEUE_CAP);
        buffer::Pool::<buffer::pool::state::Uninitialized>::try_new(
            limits.max_retained_recv_chunks() + 1,
            limits.max_recv_len(),
        )
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
    }

    fn prepare_open<'a, 'd, const ID: u8>(
        _: &'a mut Self::RuntimeContext<'d, ID>,
    ) -> Result<Option<Self::Open<'a, 'd, ID>>, Infallible>
    where
        'd: 'a,
    {
        Ok(Some(ReadyOpen::new(Self, ())))
    }

    fn process_recv<'a, 'd, const ID: u8>(
        _: &mut Self::Connection<'d, ID>,
        runtime: &mut Self::RuntimeContext<'d, ID>,
        bytes: &'a mut [u8],
        _: &wire::batch::Capacity<Self>,
    ) -> Self::RecvBatch<'a>
    where
        'd: 'a,
    {
        let chunk = (|| {
            let mut lease = runtime.try_acquire()?;
            lease.try_extend(bytes).ok()?;
            Some(RecvChunk::Owned(Bytes::<Retained>::from(lease.freeze())))
        })();
        chunk.into_iter()
    }

    fn process_retained_recv<'a, 'd, const ID: u8>(
        _: &mut Self::Connection<'d, ID>,
        runtime: &mut Self::RuntimeContext<'d, ID>,
        bytes: Lease<'a>,
    ) -> Option<Self::RetainedRecv<'a>>
    where
        'd: 'a,
    {
        let mut lease = runtime.try_acquire()?;
        lease.try_extend(bytes.as_slice()).ok()?;
        Some(Bytes::<Retained>::from(lease.freeze()))
    }

    fn prepare_send<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        _: Storage<'a, Self::StorageBackend<'d>>,
        plain: Plain<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        Prepared::input(plain)
    }

    fn prepare_send_vectored<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        _: Storage<'a, Self::StorageBackend<'d>>,
        plain: Vectored<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        Prepared::vectored(plain)
    }

    fn after_send<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, Self::StorageBackend<'d>>,
        _: Sent,
    ) -> Transition<'a, Self::Reclaim> {
        Transition::completed(send)
    }

    fn flush_pending<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, Self::StorageBackend<'d>>,
    ) -> Prepared<'a, Self::Reclaim> {
        send.empty()
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
fn receive_hoarder_cannot_starve_another_connection() {
    let addr = dope_test::peer::Peer::reserve().addr();
    let exec = dope::runtime::executor::Executor::new(
        settings::Config::for_tcp_profile::<Balanced>(MAX_CONN)
            .expect("driver config")
            .with_receive(settings::Receive::fixed::<1024, 1>()),
    )
    .expect("executor")
    .with_factory(ListenerPort::<PooledWire>::factory(MAX_CONN).expect("listener capacity"));
    exec.try_enter(|mut sess| {
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
        let clients = std::thread::spawn(move || {
            let mut hoarder =
                dope_test::peer::Peer::at(addr).connect_with_read_timeout(Duration::from_secs(3));
            let mut probe =
                dope_test::peer::Peer::at(addr).connect_with_read_timeout(Duration::from_secs(3));
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
        let dispatcher = App {
            pool: listener,
            driver: ::core::marker::PhantomData,
        };
        let result = sess
            .with_app(dispatcher, |mut app| {
                let hoarder = dope_test::fibers::TEST
                    .drive(
                        &mut app,
                        dope_gen::fiber!('_, crate = ::dope_fiber => async move { pool.accept().await }),
                    )
                    .expect("accept hoarder");
                let probe = dope_test::fibers::TEST
                    .drive(
                        &mut app,
                        dope_gen::fiber!('_, crate = ::dope_fiber => async move { pool.accept().await }),
                    )
                    .expect("accept probe");
                let mut probe = Some(probe);
                let (probe, result) = dope_test::fibers::TEST
                    .drive(
                        &mut app,
                        dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                            let mut probe = probe.take().expect("probe owner");
                            let mut request = probe.read().await?.ok_or_else(|| {
                                std::io::Error::from(std::io::ErrorKind::UnexpectedEof)
                            })?;
                            let byte = request.chunk()[0];
                            assert_eq!(request.consume(1), 1);
                            drop(request);
                            Ok::<_, std::io::Error>((probe, byte))
                        }),
                    )
                    .expect("read probe");
                let mut probe = Some(probe);
                dope_test::fibers::TEST
                    .drive(
                        &mut app,
                        dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                            let mut probe = probe.take().expect("probe owner");
                            probe.write_all(b"R").await?;
                            Ok::<_, std::io::Error>(probe)
                        }),
                    )
                    .expect("reply probe");
                drop(hoarder);
                result
            })
            .expect("application teardown");
        assert_eq!(result, b'P');
        assert_eq!(clients.join().expect("clients join"), [b'R']);
    })
    .expect("listener port storage");
}

#![cfg(target_os = "linux")]

use std::cell::Cell;
use std::net::{SocketAddr, TcpStream};
use std::pin::pin;
use std::rc::Rc;
use std::time::Duration;

use dope::fiber::Fiber;
use dope::manifold::Outcome;
use dope::manifold::env::Bundle;
use dope::manifold::listener::config::Config;
use dope::manifold::listener::{self, Application, Listener};
use dope::runtime::profile;
use dope::transport::Tcp;
use dope::transport::config::tcp::ListenerOpts;
use dope::transport::link::Slot;
use dope::transport::wire::{Identity, RecvChunk};
use dope::{Driver, DriverConfig, Executor};

struct App {
    accepted: Rc<Cell<u32>>,
}

impl Application for App {
    type Conn = ();
    type Wire = Identity;

    fn on_chunk(
        &mut self,
        _slot: &mut Slot<Self::Wire, listener::State<Self::Conn>>,
        _chunk: RecvChunk<'_>,
        _aux: &mut listener::Aux,
        _driver: &mut Driver,
    ) -> Outcome {
        Outcome::Ok
    }

    fn on_send(
        &mut self,
        _slot: &mut Slot<Self::Wire, listener::State<Self::Conn>>,
        _sent: usize,
        _aux: &mut listener::Aux,
        _driver: &mut Driver,
    ) {
    }

    fn on_accept(
        &mut self,
        _slot: &mut Slot<Self::Wire, listener::State<Self::Conn>>,
        _aux: &mut listener::Aux,
        _driver: &mut Driver,
    ) -> Outcome {
        self.accepted.set(self.accepted.get() + 1);
        Outcome::Ok
    }

    fn on_close(
        &mut self,
        _slot: &mut Slot<Self::Wire, listener::State<Self::Conn>>,
        _aux: &mut listener::Aux,
    ) {
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Host {
    #[pin]
    #[manifold]
    listener: Listener<0, App, Bundle<Tcp, Identity, profile::Throughput>>,
}

fn build(drv: &mut Driver, accepted: Rc<Cell<u32>>) -> (Host, SocketAddr) {
    let bind: SocketAddr = "127.0.0.1:0".parse().expect("parse");
    let cfg = Config::<Tcp> {
        max_conn: 16,
        bind,
        backlog: 128,
        stream_opts: Default::default(),
        listener_opts: ListenerOpts::default(),
    };
    let listener = Listener::<0, App, Bundle<Tcp, Identity, profile::Throughput>>::open_in(
        App { accepted },
        cfg,
        drv,
    )
    .expect("open_in");
    let addr = listener.local_addr().expect("local_addr");
    (Host { listener }, addr)
}

#[test]
fn driver_with_armed_recv_tears_down_cleanly() {
    let accepted = Rc::new(Cell::new(0u32));
    let mut exec = Executor::new(<dope::DriverCfg as DriverConfig>::for_tcp_profile::<
        profile::Throughput,
    >(16))
    .expect("executor");
    let (host, addr) = build(exec.driver_mut(), accepted.clone());
    let mut host = pin!(host);

    let client = std::thread::spawn(move || {
        let s = TcpStream::connect_timeout(&addr, Duration::from_millis(500)).expect("connect");
        std::thread::sleep(Duration::from_millis(200));
        s
    });

    let done = accepted.clone();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let fiber = Fiber::new(std::future::poll_fn(move |cx| {
        if done.get() >= 1 || std::time::Instant::now() >= deadline {
            std::task::Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }));
    dope_extra::block_on(&mut exec, host.as_mut(), fiber);

    assert_eq!(
        accepted.get(),
        1,
        "the connection must be accepted and its recv armed"
    );

    let stream = client.join().expect("client");
    drop(host);
    drop(exec);
    drop(stream);
}

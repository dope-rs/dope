use std::{
    cell::RefCell, convert::Infallible, net::TcpListener, rc::Rc, sync::mpsc, thread,
    time::Duration,
};

use dope_core::driver::settings;
use dope_manifold::{
    Bundle,
    connector::{
        codec::{Codec, Parse},
        lifecycle::{CloseReason, Stateless, TimeoutKind},
        session::{Ctx, Retirement, Scheduling, Session, Target},
    },
    timing::{Policy, Window},
};
use dope_net::{link::egress, tcp::Tcp, wire::Identity};
use dope_test::{fibers::Gate, scenario::scenarios::Connector};
use o3::buffer::storage::Shared;

const MAX: usize = 1;
const PAYLOAD_BYTES: usize = 32 * 1024 * 1024;

struct SendProfile;

impl settings::Profile for SendProfile {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 65_536>();
}

impl Policy for SendProfile {
    const CONNECT_DEADLINE: Window = Window::from_secs(2);
    const IDLE_WINDOW: Window = Window::from_secs(2);
    const SEND_DEADLINE: Window = Window::from_millis(100);
    const ABS_CONN_AGE: Window = Window::from_secs(5);
}

struct NeedMore;

impl Codec for NeedMore {
    type Head<'input, 'd> = ();
    type ParseState = ();
    type Error = Infallible;

    fn parse_state(&self) {}

    fn parse<'input, 'd, R: dope_net::wire::Cursor<'d>>(
        &self,
        _state: &mut Self::ParseState,
        _buf: dope_manifold::connector::codec::Input<'input, 'd, R>,
    ) -> Result<Parse<Self::Head<'input, 'd>>, Self::Error>
    where
        'd: 'input,
    {
        Ok(Parse::NeedMore)
    }

    fn finish<'d>(
        &self,
        _state: &mut Self::ParseState,
        _remaining: dope_net::wire::RetainedBytes<'d>,
    ) -> Result<Option<Self::Head<'d, 'd>>, Self::Error> {
        Ok(None)
    }
}

struct SendSession {
    codec: NeedMore,
    payload: Shared,
    closed: Gate,
    reasons: Rc<RefCell<Vec<CloseReason>>>,
}

impl<'d> Session<'d> for SendSession {
    type Codec = NeedMore;
    type ConnState = Stateless;
    type Send = Shared;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }

    fn connect(&mut self, _peer: dope_core::io::socket::Addr, ctx: &mut Ctx<'_, 'd, Self>) {
        ctx.sink
            .try_enqueue(ctx.region, self.payload.clone())
            .expect("test egress capacity");
    }

    fn response<'input>(&mut self, _head: (), _ctx: &mut Ctx<'_, 'd, Self>)
    where
        'd: 'input,
    {
    }
}

impl<'d> Retirement<'d> for SendSession {
    fn disconnect(&mut self, _ctx: &mut Ctx<'_, 'd, Self>, reason: CloseReason) {
        self.reasons.borrow_mut().push(reason);
        self.closed.hit();
    }
}

impl<'d> Scheduling<'d> for SendSession {}

impl<'d> Target<'d, 0, MAX> for SendSession {}

#[test]
fn send_timeout_cancels_the_inflight_kernel_operation() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind silent peer");
    let addr = listener.local_addr().expect("peer address");
    let (release_tx, release_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept connector");
        release_rx.recv().expect("release peer");
        drop(stream);
    });
    let closed = Gate::new();
    let reasons = Rc::new(RefCell::new(Vec::new()));

    Connector::<MAX>::new(addr, Duration::from_secs(1))
        .egress(egress::Config::shared(4, PAYLOAD_BYTES as u32))
        .run::<0, _, Bundle<Tcp, Identity, SendProfile>, _>(
            SendSession {
                codec: NeedMore,
                payload: Shared::from(vec![0; PAYLOAD_BYTES]),
                closed: closed.clone(),
                reasons: reasons.clone(),
            },
            |case| case.until(&closed, 1),
        );

    assert_eq!(
        reasons.borrow().first(),
        Some(&CloseReason::Timeout(TimeoutKind::Send))
    );
    release_tx.send(()).expect("release peer");
    server.join().expect("server join");
}

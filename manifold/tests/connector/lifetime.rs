use std::{cell::RefCell, convert::Infallible, rc::Rc, time::Duration};

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
use dope_net::{tcp::Tcp, wire::Identity};
use dope_test::{fibers::Gate, peer::Peer, scenario::scenarios::Connector};
use o3::buffer::storage::Shared;

const MAX: usize = 1;
const LIFETIME: Window = Window::from_millis(120);

struct LifetimeProfile;

impl settings::Profile for LifetimeProfile {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 65_536>();
}

impl Policy for LifetimeProfile {
    const CONNECT_DEADLINE: Window = Window::from_secs(2);
    const IDLE_WINDOW: Window = Window::from_secs(2);
    const SEND_DEADLINE: Window = Window::from_secs(2);
    const ABS_CONN_AGE: Window = LIFETIME;
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

struct LifetimeSession {
    codec: NeedMore,
    connected: Gate,
    closed: Rc<RefCell<Vec<CloseReason>>>,
}

impl<'d> Session<'d> for LifetimeSession {
    type Codec = NeedMore;
    type ConnState = Stateless;
    type Send = Shared;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }

    fn connect(&mut self, _peer: dope_core::io::socket::Addr, _ctx: &mut Ctx<'_, 'd, Self>) {
        self.connected.hit();
    }

    fn response<'input>(&mut self, _head: (), _ctx: &mut Ctx<'_, 'd, Self>)
    where
        'd: 'input,
    {
    }
}

impl<'d> Retirement<'d> for LifetimeSession {
    fn disconnect(&mut self, _ctx: &mut Ctx<'_, 'd, Self>, reason: CloseReason) {
        self.closed.borrow_mut().push(reason);
    }
}

impl<'d> Scheduling<'d> for LifetimeSession {}

impl<'d> Target<'d, 0, MAX> for LifetimeSession {}

#[test]
fn absolute_connection_age_is_typed_and_recoverable() {
    let (addr, server) = Peer::hold(2);
    let connected = Gate::new();
    let closed = Rc::new(RefCell::new(Vec::new()));

    Connector::<MAX>::new(addr, Duration::from_millis(20))
        .run::<0, _, Bundle<Tcp, Identity, LifetimeProfile>, _>(
            LifetimeSession {
                codec: NeedMore,
                connected: connected.clone(),
                closed: closed.clone(),
            },
            |case| case.until(&connected, 2),
        );

    server.join().expect("server join");
    assert!(
        closed
            .borrow()
            .contains(&CloseReason::Timeout(TimeoutKind::Lifetime)),
        "connection must retire at {:?} with the lifetime timeout kind",
        LIFETIME.get()
    );
}

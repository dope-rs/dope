#![cfg(not(target_os = "linux"))]

extern crate dope;

use std::pin::{Pin, pin};

use dope::io::AcceptEvent;
use dope::manifold::listener::application::{Application, ApplicationHooks};
use dope::manifold::listener::state::EgressCtx;
use dope::manifold::{Manifold, Outcome, listener};
use dope::{Completion, Event};
use dope_net::link::slot::Slot;
use dope_net::wire::identity::Identity;
use o3::buffer::RetainBytes;

struct App;

impl<'d> Application<'d> for App {
    type Conn = ();
    type Wire = Identity;
    type Hooks = Self;
}

impl<'d> ApplicationHooks<'d, App> for App {
    fn chunk<R: RetainBytes>(
        _app: Pin<&mut App>,
        _slot: &mut Slot<'d, Identity, listener::state::State<()>>,
        _egress: EgressCtx<'_, 'd, '_>,
        _chunk: R,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        Outcome::Ok
    }
}

#[test]
fn accept_cancel_completes_the_armed_target() {
    let (exec, cfg) = dope_test::tcp_host(8, Default::default());
    exec.enter(|mut session| {
        let hash_builder = session.seed().derive(dope::hash::domain::ACCEPT).state();
        let egress = session.storage();
        let mut driver = session.driver_access();
        let (listener, _) = dope_test::open_listener::<0, _, dope_test::Plain>(
            App,
            cfg,
            hash_builder,
            egress,
            &mut driver,
        );
        let mut listener = pin!(listener);

        Manifold::pre_park(listener.as_mut(), &mut driver);
        Manifold::shutdown(listener.as_mut(), &mut driver);

        let mut cqes = [const { None }; 2];
        let n = driver.drain(&mut cqes);
        assert_eq!(n, 1);
        let event = cqes[0].as_ref().expect("completion");
        assert!(matches!(
            event,
            Event::Accept(_, _, AcceptEvent::Failed(errno)) if *errno == libc::ECANCELED
        ));
    });
}

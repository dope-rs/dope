#![cfg(not(target_os = "linux"))]

extern crate dope;

use dope_test as common;

use std::pin::{Pin, pin};

use dope::Completion;
use dope::manifold::Manifold;
use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application};
use dope_net::link::slot::Slot;
use dope_net::wire::identity::Identity;
use o3::buffer::RetainBytes;

struct App;

impl<'d> Application<'d> for App {
    type Conn = ();
    type Wire = Identity;

    fn chunk<R: RetainBytes>(
        self: Pin<&mut Self>,
        _slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _chunk: R,
        _aux: &mut listener::Aux,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        Outcome::Ok
    }

    fn send(
        self: Pin<&mut Self>,
        _slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _sent: usize,
        _aux: &mut listener::Aux,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) {
    }

    fn close(
        self: Pin<&mut Self>,
        _slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _aux: &mut listener::Aux,
    ) {
    }
}

#[test]
fn accept_cancel_completes_the_armed_target() {
    let (exec, cfg) = common::tcp_host(8, Default::default());
    exec.enter(|mut session| {
        let hash_builder = session.seed().derive(dope::hash::domain::ACCEPT).state();
        let mut driver = session.driver_access();
        let (listener, _) =
            common::open_listener::<0, _, common::Plain>(App, cfg, hash_builder, &mut driver);
        let mut listener = pin!(listener);

        Manifold::pre_park(listener.as_mut(), &mut driver);
        Manifold::shutdown(listener.as_mut(), &mut driver);

        let mut cqes = [const { None }; 2];
        let n = driver.drain(&mut cqes);
        assert_eq!(n, 1);
        let event = cqes[0].as_ref().expect("completion");
        assert_eq!(event.operation(), dope::driver::token::kind::ACCEPT);
        assert_eq!(event.result(), -libc::ECANCELED);
    });
}

use std::cell::Cell;
use std::rc::Rc;

use dope::driver::token::{Epoch, SlotIndex, Token};
use dope::manifold::connector::port::Port;
use dope_net::link::egress::StaticBytes;

struct DropNotice {
    dropped: Rc<Cell<bool>>,
}

impl Drop for DropNotice {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}

#[test]
fn activation_releases_detached_payload_before_reuse() {
    dope_test::with_session(|mut session| {
        let current = dope_test::tok(0);
        let replacement = Token::new(
            0,
            SlotIndex::ZERO,
            Epoch::INITIAL.next().expect("replacement epoch"),
        );
        let mut driver = session.driver_access();
        let ready = driver
            .driver_ref()
            .make_ready_slot(current)
            .expect("ready slot");
        let port = Rc::new(Port::with_capacity(
            1,
            driver.region_token_ref(),
            driver.driver_ref(),
        ));
        assert!(port.activate(driver.region_token(), current, ready.key()));

        let dropped = Rc::new(Cell::new(false));
        assert!(
            port.try_enqueue(
                driver.region_token(),
                current,
                StaticBytes::new(
                    b"x",
                    DropNotice {
                        dropped: dropped.clone(),
                    },
                ),
            )
            .is_ok()
        );

        assert!(port.activate(driver.region_token(), replacement, ready.key()));
        assert!(dropped.get());

        let mut drained = 0;
        assert!(
            port.try_enqueue(
                driver.region_token(),
                replacement,
                StaticBytes::new(
                    b"x",
                    DropNotice {
                        dropped: dropped.clone(),
                    },
                ),
            )
            .is_ok()
        );
        port.drain_requests(driver.region_token(), replacement, |_, value| {
            drained += 1;
            drop(value);
            Ok(())
        })
        .expect("replacement activation");
        assert_eq!(drained, 1);
    });
}

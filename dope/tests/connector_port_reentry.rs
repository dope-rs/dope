use std::cell::Cell;
use std::rc::{Rc, Weak};

use dope::driver::token::{Epoch, SlotIndex, Token};
use dope::manifold::connector::port::Port;

struct DropReentry<'d> {
    port: Weak<Port<'d, DropReentry<'d>>>,
    token: Token,
    entered: Rc<Cell<bool>>,
    accepted: Rc<Cell<bool>>,
}

impl AsRef<[u8]> for DropReentry<'_> {
    fn as_ref(&self) -> &[u8] {
        b"x"
    }
}

impl Drop for DropReentry<'_> {
    fn drop(&mut self) {
        if self.entered.replace(true) {
            return;
        }
        let Some(port) = self.port.upgrade() else {
            return;
        };
        let value = Self {
            port: self.port.clone(),
            token: self.token,
            entered: self.entered.clone(),
            accepted: self.accepted.clone(),
        };
        self.accepted
            .set(port.try_enqueue(self.token, value).is_ok());
    }
}

#[test]
fn detached_payload_drop_can_enqueue_into_the_new_activation() {
    dope_test::with_session(|session| {
        let current = dope_test::tok(0);
        let replacement = Token::new(
            0,
            SlotIndex::ZERO,
            Epoch::INITIAL.next().expect("replacement epoch"),
        );
        let ready = session
            .driver()
            .make_ready_slot(current)
            .expect("ready slot");
        let port = Rc::new(Port::with_capacity(1, session.driver()));
        assert!(port.activate(current, ready.key()));

        let entered = Rc::new(Cell::new(false));
        let accepted = Rc::new(Cell::new(false));
        assert!(
            port.try_enqueue(
                current,
                DropReentry {
                    port: Rc::downgrade(&port),
                    token: replacement,
                    entered: entered.clone(),
                    accepted: accepted.clone(),
                },
            )
            .is_ok()
        );

        assert!(port.activate(replacement, ready.key()));
        assert!(entered.get());
        assert!(accepted.get());

        let mut drained = 0;
        port.drain_requests(replacement, |value| {
            drained += 1;
            drop(value);
            Ok(())
        })
        .expect("replacement activation");
        assert_eq!(drained, 1);
    });
}

use std::cell::Cell;
use std::rc::{Rc, Weak};

extern crate dope;
use dope::driver::ready::ReadyKey;
use dope::driver::token::{Epoch, SlotIndex, Token};
use dope::manifold::connector::{CloseKind, Port, Requests};
use o3::buffer::Shared;

mod common;
use common::assert_unwinds;

fn with_port_env<R>(f: impl for<'d> FnOnce(Token, ReadyKey<'d>, dope::DriverRef<'d>) -> R) -> R {
    common::with_session(|sess| {
        let token = common::tok(0);
        let slot = std::pin::pin!(sess.driver().make_ready_slot(token));
        let wake = slot.as_ref().key();
        f(token, wake, sess.driver())
    })
}

fn token_chain<const N: usize>() -> [Token; N] {
    let mut epoch = Epoch::INITIAL;
    std::array::from_fn(|_| {
        let token = Token::new(0, SlotIndex::new(0), epoch);
        epoch = epoch.next().expect("epoch exhausted");
        token
    })
}

fn active_port<'d, B: AsRef<[u8]>>(
    capacity: usize,
    token: Token,
    wake: ReadyKey<'d>,
    driver: dope::DriverRef<'d>,
) -> Rc<Port<'d, B>> {
    let port = Rc::new(Port::with_capacity(capacity, driver));
    assert!(port.activate(token, wake));
    port
}

fn drain_collect<B: AsRef<[u8]>>(port: &Port<'_, B>, token: Token) -> (Vec<B>, Requests) {
    let mut values = Vec::new();
    let requests = port
        .drain_requests(token, |value| {
            values.push(value);
            Ok(())
        })
        .expect("active binding");
    (values, requests)
}

fn assert_drain_empty<B: AsRef<[u8]>>(port: &Port<'_, B>, token: Token) {
    assert!(drain_collect(port, token).0.is_empty());
}

#[derive(Clone, Copy)]
enum AsRefAction {
    Close,
    Deactivate,
    CloseDeactivateReactivate,
    Reactivate,
    Panic,
    CloseDeactivateReactivatePanic,
}

struct AsRefReentry<'d> {
    port: Weak<Port<'d, AsRefReentry<'d>>>,
    current: Token,
    replacement: Token,
    wake: ReadyKey<'d>,
    action: AsRefAction,
    reentered: Rc<Cell<bool>>,
}

fn send_reentry<'d>(
    port: &Rc<Port<'d, AsRefReentry<'d>>>,
    current: Token,
    replacement: Token,
    wake: ReadyKey<'d>,
    action: AsRefAction,
    armed: bool,
) -> (bool, Rc<Cell<bool>>) {
    let reentered = Rc::new(Cell::new(!armed));
    let value = AsRefReentry {
        port: Rc::downgrade(port),
        current,
        replacement,
        wake,
        action,
        reentered: reentered.clone(),
    };
    let accepted = port.try_enqueue(current, value).is_ok();
    (accepted, reentered)
}

impl AsRef<[u8]> for AsRefReentry<'_> {
    fn as_ref(&self) -> &[u8] {
        if self.reentered.replace(true) {
            return b"outer";
        }
        let port = self.port.upgrade().expect("port");
        assert!(!port.is_active(self.current));
        match self.action {
            AsRefAction::Close => {
                port.close(self.current);
                assert!(port.is_active(self.current));
            }
            AsRefAction::Deactivate => {
                port.deactivate(self.current);
                assert!(!port.is_active(self.current));
            }
            AsRefAction::CloseDeactivateReactivate => {
                port.close(self.current);
                assert!(port.is_active(self.current));
                port.deactivate(self.current);
                assert!(!port.is_active(self.current));
                assert!(port.activate(self.replacement, self.wake));
            }
            AsRefAction::Reactivate => {
                assert!(port.activate(self.replacement, self.wake));
            }
            AsRefAction::Panic => panic!("as_ref panic"),
            AsRefAction::CloseDeactivateReactivatePanic => {
                port.close(self.current);
                assert!(port.is_active(self.current));
                port.deactivate(self.current);
                assert!(!port.is_active(self.current));
                assert!(port.activate(self.replacement, self.wake));
                panic!("as_ref reentry panic");
            }
        }
        b"outer"
    }
}

#[derive(Clone, Copy)]
enum DropAction {
    Requeue,
    Reactivate,
    CloseDeactivateReactivate,
}

struct DropReentry<'d> {
    port: Weak<Port<'d, DropReentry<'d>>>,
    target: Token,
    replacement: Token,
    wake: ReadyKey<'d>,
    action: DropAction,
    reentered: Rc<Cell<bool>>,
}

fn send_drop_reentry<'d>(
    port: &Rc<Port<'d, DropReentry<'d>>>,
    at: Token,
    target: Token,
    replacement: Token,
    wake: ReadyKey<'d>,
    action: DropAction,
) -> Rc<Cell<bool>> {
    let reentered = Rc::new(Cell::new(false));
    let value = DropReentry {
        port: Rc::downgrade(port),
        target,
        replacement,
        wake,
        action,
        reentered: reentered.clone(),
    };
    assert!(port.try_enqueue(at, value).is_ok());
    reentered
}

impl AsRef<[u8]> for DropReentry<'_> {
    fn as_ref(&self) -> &[u8] {
        b"drop"
    }
}

impl Drop for DropReentry<'_> {
    fn drop(&mut self) {
        if self.reentered.replace(true) {
            return;
        }
        let Some(port) = self.port.upgrade() else {
            return;
        };
        match self.action {
            DropAction::Requeue => {
                let _ = port.try_enqueue(
                    self.target,
                    DropReentry {
                        port: self.port.clone(),
                        target: self.target,
                        replacement: self.replacement,
                        wake: self.wake,
                        action: DropAction::Requeue,
                        reentered: self.reentered.clone(),
                    },
                );
            }
            DropAction::Reactivate => {
                assert!(port.activate(self.replacement, self.wake));
            }
            DropAction::CloseDeactivateReactivate => {
                port.close(self.target);
                port.deactivate(self.target);
                assert!(port.activate(self.replacement, self.wake));
            }
        }
    }
}

#[test]
fn as_ref_close_and_deactivate_advance_suspended_generation() {
    with_port_env(|current, wake, driver| {
        let [_, replacement] = token_chain();

        let close_port = active_port(1, current, wake, driver);
        let (accepted, reentered) = send_reentry(
            &close_port,
            current,
            replacement,
            wake,
            AsRefAction::Close,
            true,
        );
        assert!(!accepted);
        assert!(reentered.get());
        assert!(close_port.is_active(current));
        let (restored, requests) = drain_collect(&close_port, current);
        assert!(restored.is_empty());
        assert_eq!(requests.close, Some(CloseKind::Reconnect));

        let deactivate_port = active_port(1, current, wake, driver);
        let (accepted, reentered) = send_reentry(
            &deactivate_port,
            current,
            replacement,
            wake,
            AsRefAction::Deactivate,
            true,
        );
        assert!(!accepted);
        assert!(reentered.get());
        assert!(!deactivate_port.is_active(current));
        assert!(deactivate_port.activate(replacement, wake));
        assert_drain_empty(&deactivate_port, replacement);
    });
}

#[test]
fn as_ref_and_drain_reentry_preserve_latest_activation() {
    with_port_env(|current, wake, driver| {
        let [_, replacement] = token_chain();

        let as_ref_port = active_port(1, current, wake, driver);
        let (accepted, reentered) = send_reentry(
            &as_ref_port,
            current,
            replacement,
            wake,
            AsRefAction::CloseDeactivateReactivate,
            true,
        );
        assert!(!accepted);
        assert!(reentered.get());
        assert!(!as_ref_port.is_active(current));
        assert!(as_ref_port.is_active(replacement));

        let drain_port: Rc<Port<'_, &'static [u8]>> = active_port(1, current, wake, driver);
        drain_port.try_enqueue(current, b"queued").unwrap();
        let requests = drain_port
            .drain_requests(current, |value| {
                assert!(!drain_port.is_active(current));
                drain_port.close(current);
                assert!(drain_port.is_active(current));
                drain_port.deactivate(current);
                assert!(!drain_port.is_active(current));
                assert!(drain_port.activate(replacement, wake));
                Err(value)
            })
            .expect("initial binding");
        assert_eq!(requests.close, None);
        assert!(!drain_port.is_active(current));
        assert!(drain_port.is_active(replacement));
        assert_drain_empty(&drain_port, replacement);
    });
}

#[test]
fn detached_drop_reentry_cannot_be_overwritten() {
    with_port_env(|current, wake, driver| {
        let [_, transition, replacement] = token_chain();
        let port = active_port(1, current, wake, driver);
        let reentered = send_drop_reentry(
            &port,
            current,
            transition,
            replacement,
            wake,
            DropAction::CloseDeactivateReactivate,
        );
        assert!(!port.activate(transition, wake));
        assert!(reentered.get());
        assert!(!port.is_active(current));
        assert!(!port.is_active(transition));
        assert!(port.is_active(replacement));
    });
}

#[test]
fn sender_panic_restores_current_or_preserves_reentrant_activation() {
    with_port_env(|current, wake, driver| {
        let [_, replacement] = token_chain();

        let restored = active_port(1, current, wake, driver);
        assert_unwinds(|| {
            send_reentry(
                &restored,
                current,
                replacement,
                wake,
                AsRefAction::Panic,
                true,
            )
        });
        assert!(restored.is_active(current));
        let (accepted, _) = send_reentry(
            &restored,
            current,
            replacement,
            wake,
            AsRefAction::Panic,
            false,
        );
        assert!(accepted);
        let (drained, _) = drain_collect(&restored, current);
        assert_eq!(drained.len(), 1);

        let replaced = active_port(1, current, wake, driver);
        assert_unwinds(|| {
            send_reentry(
                &replaced,
                current,
                replacement,
                wake,
                AsRefAction::CloseDeactivateReactivatePanic,
                true,
            )
        });
        assert!(!replaced.is_active(current));
        assert!(replaced.is_active(replacement));
    });
}

#[test]
fn receiver_panic_restores_current_or_preserves_reentrant_activation() {
    with_port_env(|current, wake, driver| {
        let [_, replacement] = token_chain();

        let restored: Rc<Port<'_, &'static [u8]>> = active_port(1, current, wake, driver);
        restored.try_enqueue(current, b"panic").unwrap();
        assert_unwinds(|| {
            restored.drain_requests(current, |_| -> Result<(), &'static [u8]> {
                panic!("receiver panic");
            });
        });
        assert!(restored.is_active(current));
        restored.try_enqueue(current, b"next").unwrap();
        let (drained, _) = drain_collect(&restored, current);
        assert_eq!(drained, [b"next" as &[u8]]);

        let replaced: Rc<Port<'_, &'static [u8]>> = active_port(1, current, wake, driver);
        replaced.try_enqueue(current, b"panic").unwrap();
        assert_unwinds(|| {
            replaced.drain_requests(current, |_| -> Result<(), &'static [u8]> {
                assert!(!replaced.is_active(current));
                replaced.close(current);
                assert!(replaced.is_active(current));
                replaced.deactivate(current);
                assert!(!replaced.is_active(current));
                assert!(replaced.activate(replacement, wake));
                panic!("receiver reentry panic");
            });
        });
        assert!(!replaced.is_active(current));
        assert!(replaced.is_active(replacement));
        replaced.try_enqueue(replacement, b"next").unwrap();
        let (drained, _) = drain_collect(&replaced, replacement);
        assert_eq!(drained, [b"next" as &[u8]]);
    });
}

#[test]
fn connector_port_restores_rejected_front() {
    with_port_env(|token, wake, driver| {
        let port = active_port(1, token, wake, driver);
        assert!(
            port.try_enqueue(token, Shared::copy_from_slice(b"first"))
                .is_ok()
        );
        assert!(
            port.try_enqueue(token, Shared::copy_from_slice(b"second"))
                .is_ok()
        );
        port.with_receiver(token, |receiver| {
            receiver.drain(Err);
        })
        .expect("receiver");
        let mut drained = Vec::new();
        port.with_receiver(token, |receiver| {
            receiver.drain(|value| {
                drained.push(value);
                Ok(())
            });
        })
        .expect("receiver");
        assert_eq!(drained[0].as_ref(), b"first");
        assert_eq!(drained[1].as_ref(), b"second");
    });
}

#[test]
fn connector_queue_drop_detaches_before_user_destructor_reentry() {
    with_port_env(|token, wake, driver| {
        let port = active_port(1, token, wake, driver);
        let reentered = send_drop_reentry(&port, token, token, token, wake, DropAction::Requeue);
        port.deactivate(token);
        assert!(reentered.get());
        assert!(!port.is_active(token));
    });
}

#[test]
fn connector_slot_generation_rejects_stale_reentrant_transitions() {
    with_port_env(|old, wake, driver| {
        let [_, replacement] = token_chain();

        let as_ref_port = active_port(1, old, wake, driver);
        let (accepted, reentered) = send_reentry(
            &as_ref_port,
            old,
            replacement,
            wake,
            AsRefAction::Reactivate,
            true,
        );
        assert!(!accepted);
        assert!(reentered.get());
        assert!(!as_ref_port.is_active(old));
        assert!(as_ref_port.is_active(replacement));
        assert_drain_empty(&as_ref_port, replacement);

        let drop_port = active_port(1, old, wake, driver);
        let reentered = send_drop_reentry(
            &drop_port,
            old,
            old,
            replacement,
            wake,
            DropAction::Reactivate,
        );
        assert!(!drop_port.activate(replacement, wake));
        assert!(reentered.get());
        assert!(drop_port.is_active(replacement));

        let drain_port = active_port(1, old, wake, driver);
        drain_port
            .try_enqueue(old, Shared::copy_from_slice(b"stale"))
            .unwrap();
        drain_port
            .drain_requests(old, |value| {
                assert!(drain_port.activate(replacement, wake));
                Err(value)
            })
            .unwrap();
        assert_drain_empty(&drain_port, replacement);
    });
}

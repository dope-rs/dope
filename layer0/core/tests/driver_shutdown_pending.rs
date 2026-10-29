use std::{
    cell::Cell,
    convert::Infallible,
    io::{Read as _, Write as _},
    ops::ControlFlow,
    os::unix::net::UnixStream,
    time::Duration,
};

use dope_core::{
    driver::{
        life::{creation, domain, recovery},
        result::poll::{Dispatch, Drain},
    },
    io::{Reclaimed, event::Kind},
};

use crate::common::{driver::Scoped as _, rt::Runtime};

pub mod common;

#[test]
fn reports_pending() {
    let counts = [false, true].map(|signal| {
        let (source, mut notify) = UnixStream::pair().expect("shutdown pair");
        let mut attempt = creation::Attempt::new();
        let driver = attempt
            .try_driver(Runtime::quic::<2, 8>().config())
            .map_err(|error| error.kind())
            .expect("driver");
        let mut domain = domain::Domain::new(driver)
            .fd(source)
            .expect("shutdown registration");
        let seen = Cell::new(0);
        assert!(
            domain
                .start()
                .expect("fresh generation")
                .enter_with(
                    (),
                    |mut scope, _, mut source| {
                        if signal {
                            notify.write_all(&[1]).expect("shutdown signal");
                            scope.run_turn(|mut driver, turn| {
                                assert!(
                                    driver
                                        .wait(turn.reactor(), Some(Duration::from_secs(1)))
                                        .is_ok()
                                );
                            });
                        }
                        scope.finish(false, |_, _| {
                            if signal {
                                source.read_exact(&mut [0]).expect("drain before rearm");
                            }
                        });
                        assert_eq!(seen.get(), 0);
                        if !signal {
                            notify.write_all(&[1]).expect("resumed shutdown");
                            scope.run_turn(|mut driver, turn| {
                                driver
                                    .wait(turn.reactor(), Some(Duration::from_secs(1)))
                                    .expect("resumed wait");
                                let mut delivered = 0;
                                assert!(matches!(
                                    driver.dispatch(turn.reactor(), |event, _| {
                                        assert!(matches!(event.into_kind(), Kind::Shutdown));
                                        delivered += 1;
                                        ControlFlow::Continue(())
                                    }),
                                    Dispatch::Ready {
                                        drain: Drain::Drained,
                                        retained: None
                                    }
                                ));
                                assert_eq!(delivered, 1);
                            });
                        }
                    },
                    recovery::Policy::new(
                        |event| {
                            assert!(matches!(event, Reclaimed::Shutdown));
                            seen.set(seen.get() + 1);
                        },
                        recover
                    ),
                )
                .is_ok()
        );
        seen.get()
    });
    assert_eq!(counts[1], counts[0] + 1);
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}

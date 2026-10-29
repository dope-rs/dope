use std::convert::Infallible;

pub mod common;

use std::time::Duration;

use common::{
    runtime::executor::client::{Alarm, App, Immediate, Pending},
    scenario::rt::Configured,
};
use dope_core::driver::life::{creation::Attempt, recovery};
use dope_runtime::executor::{drive, session::Completion};
use dope_test::harness::scenario::TIMEOUT;

#[test]
fn deadline_preserves_reuse() {
    let mut polls = Some(0);
    let mut attempt = Attempt::new();
    let mut finish = Completion::new();
    let mut reports = Vec::new();
    let executor = Configured::quic::<2, 8>().try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    let alarm = session
                        .driver()
                        .driver_ref()
                        .timer()
                        .try_alarm()
                        .expect("timer alarm");
                    session
                        .with_app(&mut finish, App { value: 0 }, |mut app| {
                            let (due, now) = app
                                .drive_for(TIMEOUT, Alarm { alarm, due: None })
                                .expect("future alarm drive")
                                .expect("future alarm deadline");
                            assert!(now >= due);
                            assert!(matches!(
                                app.drive_for(Duration::from_millis(1), Pending),
                                Err(drive::Error::Timeout)
                            ));
                            assert_eq!(
                                app.drive(Immediate(7, &mut polls))
                                    .expect("drive after timeout"),
                                7
                            );
                            assert!(matches!(
                                app.run_for(Duration::ZERO),
                                Err(drive::Error::Timeout)
                            ));
                        })
                        .expect("release timed execution session");
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert_eq!(polls, Some(1));
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}

pub mod common;

use std::{cell::Cell, thread, time::Duration};

use dope_core::{dope_sys::thread::timer, driver::life::recovery};
use dope_manifold::{ConfigError, app::creation, timing::watchdog};
use dope_runtime::executor::session::Completion;
use dope_test::fibers;
use o3::base::time::Positive;

use crate::common::{
    manifold::{Host, root::Root},
    recover,
    scenario::Runtime,
};

#[test]
fn admission_and_lifecycle() {
    let mut finish = Completion::new();
    let mut reports = Vec::new();

    Runtime::throughput().with_scope(
        |mut session| {
            let config = Host::config::<20, 200>().expect("watchdog config");
            match watchdog::Watchdog::new(config, &mut session.driver()) {
                Err(creation::Error::Watchdog(timer::Error::Unsupported { deadline })) => {
                    assert_eq!(deadline, Positive::from_millis::<200>());
                }
                Err(error) => panic!("watchdog admission: {error:?}"),
                Ok(watchdog) => {
                    let result = session.with_quiesce(
                        &mut finish,
                        Host { watchdog },
                        Duration::from_secs(2),
                        |mut app| {
                            fibers::TEST
                                .try_pause(&mut app, Duration::from_millis(500))
                                .expect("watchdog progress");
                        },
                    );
                    assert!(result.is_ok(), "application teardown");
                    thread::sleep(Duration::from_millis(250));

                    let start = session.driver().driver_ref().clock().now();
                    let last = start.clamp_add(Duration::MAX);
                    let watchdog = watchdog::Watchdog::new(config, &mut session.driver())
                        .expect("fresh watchdog");
                    let result = session.with_at_for(
                        &mut finish,
                        Host { watchdog },
                        start.into(),
                        Duration::from_secs(2),
                        |mut app| {
                            let polls = Cell::new(0);
                            app.drive_for(last, Duration::from_millis(100), Root(&polls))
                                .map(|fixed| fixed.into_output())
                        },
                    );
                    let driven = result.expect("application teardown");
                    assert!(
                        driven.is_ok(),
                        "saturated watchdog must reach a fixed point"
                    );
                }
            }
        },
        recovery::Policy::new(|event| reports.push(event), recover),
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn period_before_deadline() {
    let period = Positive::from_millis::<100>();
    assert!(matches!(
        watchdog::Config::new(period, period),
        Err(ConfigError::WatchdogOrder { period: actual, deadline })
            if actual == period && deadline == period
    ));
}

use std::time;

use dope_core::driver::settings;
use dope_manifold::timing::{Throughput, interval::Interval};
use dope_runtime::executor::Executor;
use dope_test::fibers::{Gate, TEST};

const ID: u8 = 41;

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[coordinate]
#[dispatcher(
    core = ::dope_core,
    manifold = ::dope_manifold,
    runtime = ::dope_runtime
)]
struct Host<'d> {
    #[pin]
    #[manifold(control)]
    interval: Interval<'d, ID>,
    #[dispatcher(state)]
    gate: Gate,
}

impl<'d> Host<'d> {
    fn new(interval: Interval<'d, ID>, gate: Gate) -> Self {
        Self { interval, gate }
    }

    fn coordinate(mut this: HostCoordinate<'_, '_, 'd>) -> dope_runtime::coordinate::Flow {
        if this.interval.take_tick() {
            this.gate.hit();
        }
        dope_runtime::coordinate::Flow::Idle
    }
}

fn interval<'d>(driver: &mut dope_core::driver::Context<'_, 'd>) -> Interval<'d, ID> {
    Interval::every_second(driver).expect("interval ready slot")
}

#[test]
fn armed_and_fired_shutdown_allow_clean_same_route_reuse() {
    let config = settings::Config::for_profile::<Throughput>().expect("driver config");
    Executor::new(config)
        .expect("executor")
        .enter(|mut session| {
            let armed = interval(&mut session.driver_access());
            session
                .with_app(Host::new(armed, Gate::new()), |mut app| {
                    TEST.pause(&mut app, time::Duration::from_millis(20));
                })
                .expect("application teardown");

            let fired_gate = Gate::new();
            let fired = interval(&mut session.driver_access());
            session
                .with_app(Host::new(fired, fired_gate.clone()), |mut app| {
                    TEST.run_until(&mut app, &fired_gate, 1)
                })
                .expect("application teardown");

            let reused_gate = Gate::new();
            let reused = interval(&mut session.driver_access());
            session
                .with_app(Host::new(reused, reused_gate.clone()), |mut app| {
                    TEST.pause(&mut app, time::Duration::from_millis(20));
                    assert_eq!(reused_gate.hits(), 0, "stale tick crossed app scopes");
                    TEST.run_until(&mut app, &reused_gate, 1);
                })
                .expect("application teardown");
        });
}

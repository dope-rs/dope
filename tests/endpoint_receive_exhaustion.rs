pub mod common;

use std::{convert::Infallible, pin, rc::Rc};

use common::{
    retry::{BIND, Events, traces::Immediate},
    runtime::pooled::{self, Capture},
};
use dope::{
    core::{
        driver::{
            life::recovery, result::SubmitError, route::KeyTag, schedule::Progress,
            settings::Routes,
        },
        io::datagram::ReceiveError,
    },
    manifold::{
        app::dispatch::raw::{Context, Manifold, Probe, Projection as _, Wakes},
        network::datagram::{Transport, hook::ingress},
    },
};
use dope_quic::{
    client::Dialer,
    runtime::endpoint::{Endpoint, client::Plan},
};
use dope_test::scope::Driver;
use o3::depot::arena::slab::Vacancy;

#[test]
fn endpoint_wakes() {
    type Owner<'d> = Endpoint<'d, 1, Capture<'d, 1>>;
    type Activation<'d> = <Owner<'d> as Manifold<'d>>::Activate;

    assert!(
        <Activation<'_> as Wakes<'_, Owner<'_>, KeyTag<1>>>::ROUTES
            == Routes::singleton::<KeyTag<1>>()
    );
    assert!(<Activation<'_> as Wakes<'_, Owner<'_>, KeyTag<1>>>::select().is_some());
}

#[test]
fn dialer_wakes() {
    type Owner<'d> = Dialer<'d, 'd, 1, Events, Immediate>;
    type Activation<'d> = <Owner<'d> as Manifold<'d>>::Activate;

    assert!(
        <Activation<'_> as Wakes<'_, Owner<'_>, KeyTag<1>>>::ROUTES
            == Routes::singleton::<KeyTag<1>>()
    );
    assert!(<Activation<'_> as Wakes<'_, Owner<'_>, KeyTag<1>>>::select().is_some());
}

#[test]
fn stops_without_output() {
    let plan = Plan::<1>::new(BIND, pooled::endpoint()).expect("endpoint plan");
    let mut reports = Vec::new();
    Driver::new(Events::settings(8, plan.as_ref()).expect("driver claims")).with_scope(
        |scope| {
            let (_, mut driver, mut control) = scope.split();
            let endpoint: Endpoint<'_, 1, _> = plan
                .bind(
                    Capture {
                        established: Rc::default(),
                        errors: Rc::default(),
                        received: Rc::default(),
                        stop: Rc::default(),
                    },
                    &mut driver,
                )
                .expect("bound endpoint");
            let mut endpoint = pin::pin!(endpoint);
            let clock = driver.driver_ref().clock();
            for (cause, runnable) in [(Vacancy::Full, false), (Vacancy::IdentityExhausted, true)] {
                <ingress::Coupled as ingress::Failure<'_, KeyTag<1>, _>>::failed(
                    endpoint.as_mut().inner_mut(),
                    ReceiveError::Submit(SubmitError::from(cause)),
                    clock.now(),
                    driver.state_token(),
                );
                let engine = endpoint.as_mut().inner_mut().handler_mut();
                let progress = Transport::progress(engine.as_ref(), clock, driver.state_token());
                assert_eq!(matches!(progress, Progress::Runnable), runnable);
            }
            let turn = control.begin();
            Manifold::shutdown(
                endpoint.as_mut().inner_mut(),
                turn.turn(),
                &mut Context::new(driver.reborrow()).narrow(),
            );
            let (engine, socket) = endpoint.as_mut().inner_mut().split();
            Transport::pre_park(
                engine,
                socket,
                clock.now(),
                turn.turn().app(),
                driver.state_token(),
            );
            let progress = <Endpoint<'_, 1, Capture<'_, 1>> as Manifold<'_>>::Probe::progress(
                endpoint.as_ref(),
                driver.regions(),
                clock,
            );
            assert!(
                matches!(progress, Progress::Quiescent),
                "stopped endpoint retains a CID rotation deadline"
            );
        },
        recovery::Policy::new(
            |event| reports.push(event),
            |failure: recovery::Recovery<'_, '_>| -> Infallible {
                panic!("native recovery: {:?}", failure.cause())
            },
        ),
        |result| {
            result
                .as_ref()
                .map_err(|error| error.cause())
                .expect("driver retirement");
        },
    );
    assert!(
        reports.is_empty(),
        "unexpected native retirement: {reports:?}"
    );
}

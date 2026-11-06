use std::{convert::Infallible, net::SocketAddr};

use dope_core::{
    driver::{
        self,
        life::recovery,
        result::poll::Close,
        route::{KeyTag, lease, table::conn},
        schedule,
    },
    io::{
        Reclaimed,
        socket::{self, option, pending},
    },
};
use dope_net::{
    Listen,
    link::{
        egress::{self, data::compose::Either, reserve::demand},
        pool::{
            self,
            change::{self, accept, close, drain::Policy},
            recv::input,
        },
    },
    tcp::{self, Tcp},
    wire::{
        Identity,
        domain::{Local, Resident, Retained},
        receive::mode::Disabled,
    },
};
use dope_test::scope;
use o3::{alloc, buffer::pool::Cursor};

use crate::common::rt;

type Route = KeyTag<23>;
type Payload<'d> = Either<&'d [u8], Cursor>;
type Pool<'p, 'd, D = demand::Fallible> =
    pool::Pool<'p, 'd, 'd, Route, Tcp, Identity, (), input::Borrowed, Payload<'d>, 32, D>;

#[derive(Debug)]
pub enum Error {
    Resource(dope_net::Resource),
    Allocation(alloc::Error),
    Route(lease::Error),
    Bind(dope_net::BindingError),
    Pending,
    Failed(pending::Cause),
    Unavailable,
    Exhausted,
}

pub fn reclaimed(report: Reclaimed) {
    assert!(
        matches!(
            report,
            Reclaimed::Close {
                slot,
                file: Close::Closed(Ok(())),
                reactor: Ok(()),
            } if slot.route().is_framework()
        ),
        "unexpected reclaimed operation: {report:?}"
    );
}

pub fn with_pool<const LANES: usize>(
    config: egress::Config,
    test: impl for<'p, 'd> FnOnce(
        &mut Pool<'p, 'd>,
        [pool::Key<'d, Route>; LANES],
        &mut driver::Context<'_, 'd>,
        schedule::Turn<'_, 'd>,
    ),
) {
    rt::throughput().with_scope(
        |scope| {
            let (_, mut driver, mut control) = scope.split();
            let turn = control.begin();
            let capacity = conn::Capacity::fixed::<LANES>();
            change::try_prepare::<
                Route,
                Tcp,
                Identity,
                (),
                input::Borrowed,
                Payload<'_>,
                32,
                _,
                _,
                _,
                _,
            >(
                Retained::ZERO,
                Resident::ZERO,
                demand::Fallible::prepare(config, capacity),
                Local::new(Identity),
                &mut driver,
                driver::Context::reborrow,
                |(prepared, need), driver| {
                    let flights = driver
                        .storage
                        .reborrow()
                        .try_flights(need)
                        .map_err(Error::Allocation)?
                        .slots();
                    let route = driver
                        .storage
                        .reborrow()
                        .try_reserve::<Route>()
                        .map_err(Error::Route)?;
                    // SAFETY: this fixture never submits send, receive, or tuning
                    // operations. Pool storage therefore has no kernel borrowers.
                    let mut pool = unsafe { prepared.commit(flights).bind(route) };
                    let mut keys = [open(&mut pool, capacity, driver)?; LANES];
                    for key in &mut keys[1..] {
                        *key = open(&mut pool, capacity, driver)?;
                    }
                    test(&mut pool, keys, driver, turn.turn());
                    Ok::<_, Error>(())
                },
            )
            .map_err(Error::Resource)?
        },
        recovery::Policy::new(
            reclaimed,
            |failure: recovery::Recovery<'_, '_>| -> Infallible {
                panic!("driver recovery: {:?}", failure.cause())
            },
        ),
        |result| {
            result
                .as_ref()
                .map_err(scope::Error::cause)
                .expect("native lifecycle");
            result.ok().expect("driver output").expect("credit fixture");
        },
    );
}

pub fn open<'d, D: demand::Mode>(
    pool: &mut Pool<'_, 'd, D>,
    capacity: conn::Capacity,
    driver: &mut driver::Context<'_, 'd>,
) -> Result<pool::Key<'d, Route>, Error> {
    let (fd, _) = Tcp::bind(
        driver,
        &SocketAddr::from(([127, 0, 0, 1], 0)),
        socket::Backlog::fixed::<1>(),
        &tcp::Config::default(),
    )
    .map_err(Error::Bind)?;
    let opened = match pool.accept_with(
        capacity,
        fd,
        option::Set::EMPTY,
        |_| Ok::<_, Infallible>(()),
        driver,
    ) {
        Ok(opened) => opened,
        Err(cause) => cause.eliminate(),
    };
    match opened {
        accept::Outcome::Ready(key) => Ok(key),
        accept::Outcome::Pending => Err(Error::Pending),
        accept::Outcome::Failed(_, cause) => Err(Error::Failed(cause)),
        accept::Outcome::Unavailable => Err(Error::Unavailable),
        accept::Outcome::IdentityExhausted => Err(Error::Exhausted),
        accept::Outcome::Rejected(cause) => match cause {},
    }
}

/// Releases the queued entry through the same close transition used by owners.
pub fn release_one<'p, 'd>(
    pool: &mut Pool<'p, 'd>,
    key: pool::Key<'d, Route>,
    driver: &mut driver::Context<'_, 'd>,
    turn: schedule::Turn<'_, 'd>,
) -> Result<bool, close::Failure> {
    let mut released = Ok(false);
    <Disabled as Policy<Identity>>::close(
        pool,
        key,
        turn.maint(),
        driver,
        close::Handler {
            claim: (),
            context: &mut released,
            owner: |_: close::Owner<
                '_,
                '_,
                '_,
                '_,
                '_,
                '_,
                'd,
                'd,
                Route,
                Identity,
                (),
                (),
                Result<bool, close::Failure>,
            >| close::Decision::Ready,
            commit: |_: close::Commit<'_, '_, '_, '_, 'd, (), Result<bool, close::Failure>, ()>| {
                close::Approval::Ready(())
            },
            failed: |(released, _, failed, _): close::Failed<
                '_,
                '_,
                '_,
                '_,
                '_,
                'p,
                'd,
                'd,
                Route,
                Identity,
                (),
                Payload<'d>,
                32,
                demand::Fallible,
                (),
                Result<bool, close::Failure>,
            >| *released = Err(failed.failure()),
        },
        |released: &mut Result<bool, close::Failure>, _: Payload<'d>, _| {
            if let Ok(released) = released {
                *released = true;
            }
        },
        |_, released, _, outcome| Ok((*released)? && matches!(outcome, close::Outcome::Pending(()))),
    )
}

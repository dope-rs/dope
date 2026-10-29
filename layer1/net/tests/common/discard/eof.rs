use std::{cell::Cell, convert::Infallible, io::Write, net::TcpStream, ops::ControlFlow, rc::Rc};

use dope_core::{
    driver::{
        life::{self, recovery},
        retained,
        route::{Erased, KeyTag, kind, table::conn},
        schedule::ready,
        settings,
    },
    io::{
        Reclaimed,
        event::{
            self,
            batch::Flow,
            net::{accept::Outcome, recv},
        },
        fd::handles,
        socket::option,
    },
};
use dope_net::{
    link::{
        egress::{self, reserve::demand},
        event::Dispatch,
        pool::{
            self,
            change::{self, accept},
            recv::input,
        },
        slot::transmit::Finalize,
    },
    tcp::Tcp,
    wire::domain::{Local, Resident, Retained},
};
use dope_test::scope::{self, Driver};

use crate::common::{
    discard::{Error, Peer, next},
    wire::{Eof, Input},
};

pub type Route = KeyTag<24>;
type Pool<'p, 'd, M> = pool::Pool<'p, 'd, 'd, Route, Tcp, Eof, (), M, &'d [u8]>;
type Opened<'d> = (pool::Key<'d, Route>, TcpStream);

pub fn run<M: input::Mode<Eof>>(
    test: impl for<'p, 'd> FnOnce(
        &mut Pool<'p, 'd, M>,
        pool::Key<'d, Route>,
        &mut life::Scope<'_, 'd>,
        TcpStream,
    ),
    recover: impl recovery::Recover,
    finish: impl for<'owner> FnOnce(Result<Result<(), Error>, scope::Error<'owner, Result<(), Error>>>),
) -> (Vec<Reclaimed>, Input) {
    run_with::<M>(
        false,
        |pool, scope, (key, peer), _| {
            test(pool, key, scope, peer);
            Ok(())
        },
        recover,
        finish,
    )
}

pub fn run_pair<M: input::Mode<Eof>>(
    test: impl for<'p, 'd> FnOnce(
        &mut Pool<'p, 'd, M>,
        [pool::Key<'d, Route>; 2],
        &mut life::Scope<'_, 'd>,
        [TcpStream; 2],
    ),
    recover: impl recovery::Recover,
    finish: impl for<'owner> FnOnce(Result<Result<(), Error>, scope::Error<'owner, Result<(), Error>>>),
) -> (Vec<Reclaimed>, Input) {
    run_with::<M>(
        true,
        |pool, scope, (a, left), other| {
            let (b, right) = other.ok_or(Error::Connection)?;
            test(pool, [a, b], scope, [left, right]);
            Ok(())
        },
        recover,
        finish,
    )
}

fn run_with<M: input::Mode<Eof>>(
    pair: bool,
    test: impl for<'p, 'd> FnOnce(
        &mut Pool<'p, 'd, M>,
        &mut life::Scope<'_, 'd>,
        Opened<'d>,
        Option<Opened<'d>>,
    ) -> Result<(), Error>,
    recover: impl recovery::Recover,
    finish: impl for<'owner> FnOnce(Result<Result<(), Error>, scope::Error<'owner, Result<(), Error>>>),
) -> (Vec<Reclaimed>, Input) {
    let mut reports = Vec::new();
    let input = Rc::new(Cell::new(Input::Empty));
    let (files, cap) = if pair {
        (
            settings::FileSlots::fixed::<2, 1>(),
            conn::Capacity::fixed::<2>(),
        )
    } else {
        (
            settings::FileSlots::fixed::<1, 1>(),
            conn::Capacity::fixed::<1>(),
        )
    };
    let config = settings::Config::default()
        .with_files(files)
        .with_receive(settings::Receive::fixed::<2, 4096>());
    Driver::new(config).with_scope(
        |scope| prepare::<M>(scope, Eof::new(Rc::clone(&input)), cap, pair, test),
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    (reports, input.get())
}

fn prepare<'d, M: input::Mode<Eof>>(
    scope: &mut life::Scope<'_, 'd>,
    wire: Eof,
    cap: conn::Capacity,
    pair: bool,
    test: impl for<'p> FnOnce(
        &mut Pool<'p, 'd, M>,
        &mut life::Scope<'_, 'd>,
        Opened<'d>,
        Option<Opened<'d>>,
    ) -> Result<(), Error>,
) -> Result<(), Error> {
    let (peer, fd) = Peer::new(scope)?;
    change::try_prepare::<Route, Tcp, Eof, (), M, &[u8], 32, _, _, _, _>(
        Retained::ZERO,
        Resident::ZERO,
        demand::Fallible::prepare(egress::Config::DEFAULT, cap),
        Local::new(wire),
        scope,
        life::Scope::context,
        |(prepared, need), scope| {
            let mut pool = {
                let mut driver = scope.context();
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
                // SAFETY: every operation below settles through mandatory native quiescence
                // and pool.finish before the pool, its flights, or submitted backing leave.
                unsafe { prepared.commit(flights).bind(route) }
            };
            let Peer {
                listener,
                flight,
                stream,
            } = peer;
            let result = open(&mut pool, fd, cap, scope).and_then(|key| {
                let other = if pair {
                    if flight.is_none() {
                        return Err(Error::Flight);
                    }
                    let peer = TcpStream::connect(stream.peer_addr().map_err(Error::Peer)?)
                        .map_err(Error::Peer)?;
                    let fd = match next(scope)?.into_kind() {
                        event::Kind::Accept(done) => {
                            let (_, _, result) = done.into_parts();
                            match result {
                                Outcome::Accepted(fd) => fd.bind(),
                                Outcome::Failed(cause) => return Err(Error::Accept(cause)),
                            }
                        }
                        other => return Err(Error::event(kind::Kind::Accept, other)),
                    };
                    Some((open(&mut pool, fd, cap, scope)?, peer))
                } else {
                    None
                };
                test(&mut pool, scope, (key, stream), other)
            });
            listener.close(&mut scope.context());
            scope.finish(true, |_, finish| pool.finish(finish, |_, _| {}));
            drop(flight);
            result
        },
    )
    .map_err(Error::Resource)?
}

fn open<'p, 'd, M: input::Mode<Eof>>(
    pool: &mut Pool<'p, 'd, M>,
    fd: handles::Descriptor<'d>,
    cap: conn::Capacity,
    scope: &mut life::Scope<'_, 'd>,
) -> Result<pool::Key<'d, Route>, Error> {
    let ready = fd.ready_handle();
    let opened = match pool.accept_with(
        cap,
        fd,
        option::Set::EMPTY,
        |_| Ok::<_, Infallible>(()),
        &mut scope.context(),
    ) {
        Ok(opened) => opened,
        Err(cause) => cause.eliminate(),
    };
    match opened {
        accept::Outcome::Ready(key) => {
            ready.set_target(key.target().dispatch());
            Ok(key)
        }
        accept::Outcome::Pending => Err(Error::Pending),
        accept::Outcome::Failed(_, cause) => Err(Error::Failed(cause)),
        accept::Outcome::Unavailable => Err(Error::Unavailable),
        accept::Outcome::IdentityExhausted => Err(Error::Exhausted),
        accept::Outcome::Rejected(cause) => match cause {},
    }
}

pub fn arm<'p, 'd, M: input::Mode<Eof>>(
    pool: &mut Pool<'p, 'd, M>,
    key: pool::Key<'d, Route>,
    scope: &mut life::Scope<'_, 'd>,
) -> Result<(), Error> {
    let mut driver = scope.context();
    let owner = retained::raw::Owner::from_driver(driver.driver_ref());
    let mut driver = retained::Context::new(driver.reborrow(), owner);
    pool.recv()
        .arm(key, &mut driver)
        .map_err(|failure| Error::ReceiveSubmit(failure.cause))
}

pub fn flush<'p, 'd, M: input::Mode<Eof>>(
    pool: &mut Pool<'p, 'd, M>,
    scope: &mut life::Scope<'_, 'd>,
) -> Result<(), Error> {
    scope.with_driver(|mut driver, mut control| {
        let turn = control.begin();
        let owner = retained::raw::Owner::from_driver(driver.driver_ref());
        let mut driver = retained::Context::new(driver.reborrow(), owner);
        pool.recv()
            .flush(turn.turn().maint(), &mut driver)
            .map_err(|failure| Error::ReceiveSubmit(failure.cause))
    })
}

pub fn receive<'d>(scope: &mut life::Scope<'_, 'd>) -> Result<(Flow, recv::Kind<'d>), Error> {
    let done = match next(scope)?.into_kind() {
        event::Kind::Recv(done) => done,
        other => return Err(Error::event(kind::Kind::Recv, other)),
    };
    let mut observed = Flow::Last;
    let done = match done.dispatch(|_, flow, value| {
        observed = flow;
        ControlFlow::<_, Infallible>::Break(value)
    }) {
        ControlFlow::Break(done) => done,
        ControlFlow::Continue(never) => match never {},
    };
    Ok((observed, done.classify()))
}

pub fn final_byte<'p, 'd, M: input::Mode<Eof>>(
    pool: &mut Pool<'p, 'd, M>,
    key: pool::Key<'d, Route>,
    scope: &mut life::Scope<'_, 'd>,
    peer: &mut TcpStream,
) -> Result<(Flow, recv::Data<'d>), Error> {
    arm(pool, key, scope)?;
    peer.write_all(b"F").map_err(Error::Peer)?;
    let (flow, item) = receive(scope)?;
    match item {
        recv::Kind::Data(bytes) => Ok((flow, bytes)),
        recv::Kind::Control(done) => Err(Error::receive(done.status())),
    }
}

pub fn cancel<'p, 'd, M: input::Mode<Eof>>(
    pool: &mut Pool<'p, 'd, M>,
    scope: &mut life::Scope<'_, 'd>,
    flow: Flow,
    observe: impl FnOnce(Flow, recv::Status, Dispatch<'d, Route>),
) -> Result<(), Error> {
    flush(pool, scope)?;
    if flow == Flow::Last {
        return Ok(());
    }
    let (flow, item) = receive(scope)?;
    let done = match item {
        recv::Kind::Control(done) => done,
        recv::Kind::Data(done) => return Err(Error::ReceiveData(done.into_parts().2.len())),
    };
    observe(flow, done.status(), pool.recv().dispatch(done));
    Ok(())
}

pub fn take_ready<'d>(
    scope: &mut life::Scope<'_, 'd>,
    mut observe: impl FnMut(Erased<'d, kind::Any>),
) -> ready::Readiness {
    scope.with_driver(|_, mut control| {
        let turn = control.begin();
        let mut found = ready::Readiness::Idle;
        turn.ready().drain(|token| {
            observe(token);
            found = ready::Readiness::Ready;
        });
        found
    })
}

pub fn end<'p, 'd, M: input::Mode<Eof>>(
    pool: &mut Pool<'p, 'd, M>,
    key: pool::Key<'d, Route>,
    scope: &mut life::Scope<'_, 'd>,
) -> Result<Finalize<()>, Error> {
    let mut driver = scope.context();
    let owner = retained::raw::Owner::from_driver(driver.driver_ref());
    let mut driver = retained::Context::new(driver.reborrow(), owner);
    let view = pool.egress_mut(key).ok_or(Error::Connection)?;
    Ok(view.connection.sending().finish(view.flights, &mut driver))
}

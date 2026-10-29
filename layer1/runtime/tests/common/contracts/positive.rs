use std::{convert::Infallible, hash::BuildHasher, pin::Pin};

use dope_core::driver::{schedule, settings, storage};
use dope_runtime::{
    client::{self, Bound, Provider},
    executor::{self, drive, root, session},
    process::{self, failure::bus},
    random::{Domain, HashState, Hasher},
    shutdown,
    topology::{self, lending, recipe},
};

fn terminal_once<'run>() {
    let _ = |terminal: process::Terminal<'run>,
             request: shutdown::Requested<process::Shutdown<'run>>|
     -> Result<bus::Event, bus::Fault> { terminal.finish(request) };
}

fn terminal_borrow<'run>() {
    let _ = |ctx: &process::Context<'run>| -> process::Terminal<'run> { ctx.terminal() };
}

fn hash_state<'d>() {
    let _ = |scope: &mut session::Scope<'_, '_, '_, 'd>, domain: Domain| -> HashState<'d> {
        scope.hash_state(domain)
    };
}

fn hasher<'d>() {
    let _ = |state: HashState<'d>| -> Hasher<'d> { state.build_hasher() };
}

struct Value;

impl<'d> executor::raw::Layout<'d> for Value {
    type ParkPlan = Infallible;
}

// SAFETY: this fixture owns no routes or native operations.
unsafe impl<'d> executor::raw::App<'d> for Value {
    const ROUTES: settings::Routes = settings::Routes::EMPTY;
    const COORDS: usize = 0;

    fn select_park() -> Option<Self::ParkPlan> {
        None
    }

    unsafe fn park<'app, 'driver, 'turn, 'root>(
        _: Pin<&'app mut Self>,
        plan: &mut Self::ParkPlan,
        _: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        match *plan {}
    }

    fn progress(_: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    fn stopping(_: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }
}

impl<'d> Provider<'d> for Value {
    type Client<'app>
        = Bound<'app, 'd, Self, u8>
    where
        Self: 'app,
        'd: 'app;

    fn provide<'app>(context: client::Context<'_, 'app, 'd, Self>) -> Self::Client<'app>
    where
        Self: 'app,
        'd: 'app,
    {
        context.into_parts().1.bind(1)
    }
}

fn owned_only<'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S, Q>(
    stage: &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>,
    report: &mut session::Completion,
) where
    S: storage::Plan + 'd,
    Q: executor::Startup,
{
    let _ = stage.with_app(report, Value, |_| ());
}

fn held_once<'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S, Q, O>(
    stage: &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>,
    owner: O,
    report: &mut session::Completion,
) where
    S: storage::Plan + 'd,
    Q: executor::Startup,
    O: lending::Root<'d> + lending::raw::Resume<'d>,
{
    stage.hold(owner, |held, stage| {
        let _ = held.lend_scope(stage, report, |issued| {
            let (_client, permit) = issued.into_parts();
            permit.reject(())
        });
    });
}

fn owned_lent<'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S, Q, O>(
    stage: &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>,
    owner: O,
    report: &mut session::Completion,
) where
    S: storage::Plan + 'd,
    Q: executor::Startup,
    O: lending::Root<'d> + lending::raw::Resume<'d>,
{
    let _ = stage.lend_scope(report, owner, |issued| {
        let (_client, permit) = issued.into_parts();
        permit.try_run(|_driver| Ok::<_, ()>((Value, 7_u8)), |data, _app| data)
    });
}

fn client_borrow<'borrow, 'app: 'borrow, 'd: 'app, P: 'borrow>() {
    let _ = |value: &'borrow Bound<'app, 'd, P>| -> Bound<'borrow, 'd, P> { value.borrow() };
}

fn storage_borrow(storage: &String) -> &String {
    match <recipe::Borrow as recipe::Build<storage::Value<String>>>::build(recipe::Borrow, storage)
    {
        Ok(value) => value,
        Err(error) => match error {},
    }
}

fn stage_borrow<'life, 'p: 'life, 'd: 'p>() {
    let _ = |stage: &topology::Stage<'_, '_, 'life, 'p, 'd, storage::Value<String>, ()>| -> &'life String {
        stage.storage()
    };
}

fn run_once<'app, 'life: 'app, 'd: 'life, D: executor::raw::App<'d> + 'life, Q: 'life>() {
    let _ =
        |app: session::App<'app, 'life, 'd, D, Q>| -> Result<shutdown::Requested<Q>, drive::Error> {
            app.run()
        };
}

fn poll_root<'a, 'd: 'a, R>(value: root::Context<'a, 'd, R>) -> Pin<&'a mut R> {
    value.into_parts().0
}

fn main() {}

struct LayoutOnly;

impl<'d> executor::raw::Layout<'d> for LayoutOnly {
    type ParkPlan = ();
}

fn shape_only<'app, 'life: 'app, 'd: 'life>(
    app: session::App<'app, 'life, 'd, LayoutOnly>,
) -> session::App<'app, 'life, 'd, LayoutOnly> {
    app
}

use dope::core::driver::life::recovery;
use dope_fiber::{
    abi::Ready,
    context::wake::RootWaker,
    task::storage::{fixed::direct::Insert, scope::Scope},
};

use crate::common::runtime::profile::Tasks;
pub mod common;

use std::{convert::Infallible, hint, mem, pin};

use dope::core::driver::{
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::ready::task::Error,
};
use dope_fiber::{
    abi,
    task::storage::fixed::{
        direct,
        local::{Removal, Store},
        quota::{Bounded, Limit},
        schedule::{
            Plane, Pool,
            result::{Outcome, Selection},
        },
        staged::Slab,
        tree::{Branch, Here, Leaf, Left, Right},
    },
};

use crate::common::{
    runtime::{self, Setup},
    schedule::quotas::*,
};

#[test]
fn direct_quota() {
    assert_eq!(
        mem::size_of::<direct::Plane<'static, LocalRoutes>>(),
        mem::size_of::<LocalRoutes>(),
    );
    let routes = LocalRoutes::default();
    let mut tasks = pin::pin!(hint::black_box(direct::Plane::bounded(
        routes,
        Limit::new(1),
    )));
    Scope::with(tasks.as_mut(), |mut tasks| {
        {
            let mut route = tasks.as_mut().route::<Left<Here>>();
            let admit = route.admit().expect("the route owns one vacant slot");
            assert_eq!(mem::size_of_val(&admit), 4 * mem::size_of::<usize>());
        }
        assert_eq!(tasks.as_ref().get_ref().available().get(), 1);

        let id = {
            let mut route = tasks.as_mut().route::<Left<Here>>();
            let admit = route.admit().expect("the route owns one vacant slot");
            Insert::insert(admit, Ready::new(()))
        };
        assert_eq!(tasks.as_ref().get_ref().available().get(), 0);
        {
            let mut route = tasks.as_mut().route::<Right<Here>>();
            assert!(route.admit().is_none());
            assert!(Store::insert(route.as_mut(), Ready::new(()),).is_none(),);
        }
        assert!(tasks.as_mut().route::<Left<Here>>().remove(id) == Removal::Removed);
        assert_eq!(tasks.as_ref().get_ref().available().get(), 1);
    });
}

#[test]
fn scheduled_quota() {
    let mut reports = Vec::new();

    Setup::for_profile::<Tasks>().with_turn(
        |turn, mut driver| {
            let rt = driver.driver_ref();
            let tags = rt.targets::<KeyTag<0>>();
            let ready = rt
                .ready()
                .make_slot(tags.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("ready slot");
            let parent = RootWaker::from(ready.target());
            let routes = Leaf::new(
                Pool::<'_, abi::Ready<()>, 2, (), u8>::try_new()
                    .expect("schedule task pool allocation"),
            );
            let mut tasks = pin::pin!(Plane::<_, Bounded>::bounded(routes, Limit::new(1)));
            Scope::with(tasks.as_mut(), |mut tasks| {
                {
                    let mut route = tasks.as_mut().route::<Here>();
                    let admit = route.try_admit((), parent);
                    assert!(admit.is_ok(), "available route");
                }
                assert_eq!(tasks.as_ref().get_ref().available().get(), 1);

                let id = tasks
                    .as_mut()
                    .route::<Here>()
                    .try_admit((), parent)
                    .expect("available route")((Ready::new(()), 7));
                assert_eq!(tasks.as_ref().get_ref().available().get(), 0);
                assert!(matches!(
                    tasks.as_mut().route::<Here>().try_admit((), parent),
                    Err(Error::Capacity { available: 0, .. }),
                ));
                assert!((tasks.as_mut().route::<Here>().remove(id)) == (Removal::Removed));
                assert_eq!(tasks.as_ref().get_ref().available().get(), 1);

                let done_id = tasks
                    .as_mut()
                    .route::<Here>()
                    .try_admit((), parent)
                    .expect("returned quota")((Ready::new(()), 9));
                let task = match tasks.as_mut().select(turn.turn().app()) {
                    Selection::Ready(task) => Some(task),
                    _ => None,
                }
                .expect("ready task must be selected");
                assert!(matches!(
                    task.only().drive(&mut driver),
                    Outcome::Complete {
                        target: 9,
                        output: (),
                    }
                ));
                assert_eq!(tasks.as_ref().get_ref().available().get(), 1);
                assert!((tasks.as_mut().route::<Here>().remove(done_id)) == (Removal::Absent));
                assert_eq!(tasks.as_ref().get_ref().available().get(), 1);
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

type Local = Slab<'static, abi::Ready<()>, 1, Domain>;
type LocalRoutes = Branch<Leaf<Local>, Leaf<Local>>;

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}

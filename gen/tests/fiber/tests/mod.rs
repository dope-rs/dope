use dope::{core::driver::settings, manifold::timing::Throughput, runtime::executor::Executor};
use fiber_rt::{abi::Ready, extensions::AppSessionExt};

mod sealed;

#[test]
fn sequential_storage_tracks_max_live_size() {
    let one = dope_gen::fiber!('_, crate = ::fiber_rt => async move {
        {
            let storage: [u8; 4096] = [1; 4096];
            Ready::new(()).await;
            storage[0] as usize
        }
    });
    let sequential = dope_gen::fiber!('_, crate = ::fiber_rt => async move {
        let first = {
            let storage: [u8; 4096] = [1; 4096];
            Ready::new(()).await;
            storage[0] as usize
        };
        let second = {
            let storage: [u8; 4096] = [2; 4096];
            Ready::new(()).await;
            storage[0] as usize
        };
        first + second
    });
    assert!(std::mem::size_of_val(&sequential) <= std::mem::size_of_val(&one) + 64);
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct App {
    #[pin]
    #[manifold]
    dummy: Dummy,
}

struct Dummy;

struct Ranking;

impl Ranking {
    fn rank<'d>(
        &self,
        user: Vec<u8>,
    ) -> impl fiber_rt::abi::Fiber<'d, Output = Result<Option<usize>, ()>> + use<'d> {
        Ready::new(Ok(Some(user.len())))
    }
}

fn executor() -> Executor {
    let config = settings::Config::for_profile::<Throughput>().expect("driver config");
    Executor::new(config).expect("executor")
}

#[test]
fn nested_await_runs_to_completion() {
    let value = executor().enter(|mut session| {
        let fiber = dope_gen::fiber!('_, crate = ::fiber_rt => async move {
            Ready::new(Ready::new(7usize).await).await
        });
        session
            .with_app(App { dummy: Dummy }, |mut app| {
                app.block_on(fiber).expect("runtime park")
            })
            .expect("application teardown")
    });
    assert_eq!(value, 7);
}

#[test]
fn fully_qualified_known_macro_lowers_await() {
    let matched = executor().enter(|mut session| {
        let fiber = dope_gen::fiber!('_, crate = ::fiber_rt => async move {
            ::core::matches!(Ready::new(1usize).await, 1)
        });
        session
            .with_app(App { dummy: Dummy }, |mut app| {
                app.block_on(fiber).expect("runtime park")
            })
            .expect("application teardown")
    });
    assert!(matched);
}

#[test]
fn unqualified_standard_macros_are_canonicalized() {
    let (values, formatted, matched) = executor().enter(|mut session| {
        let fiber = dope_gen::fiber!('_, crate = ::fiber_rt => async move {
            let values = vec![
                Ready::new(1usize).await,
                Ready::new(2usize).await,
                3,
            ];
            let formatted = format!("{}", Ready::new(7usize).await);
            let matched = matches!(Ready::new(Some(3usize)).await, Some(3));
            (values, formatted, matched)
        });
        session
            .with_app(App { dummy: Dummy }, |mut app| {
                app.block_on(fiber).expect("runtime park")
            })
            .expect("application teardown")
    });
    assert_eq!(values, [1, 2, 3]);
    assert_eq!(formatted, "7");
    assert!(matched);
}

#[test]
fn awaited_clone_keeps_owned_local_movable() {
    let (user, rank) = executor().enter(|mut session| {
        let fiber = dope_gen::fiber!('_, crate = ::fiber_rt => async move {
            let ranking = Ranking;
            let user = String::from("alice").into_bytes();
            match ranking.rank(user.clone()).await {
                Ok(Some(rank)) => (user, rank),
                Ok(None) | Err(()) => (user, 0),
            }
        });
        session
            .with_app(App { dummy: Dummy }, |mut app| {
                app.block_on(fiber).expect("runtime park")
            })
            .expect("application teardown")
    });
    assert_eq!(user, b"alice");
    assert_eq!(rank, 5);
}

use std::{convert::Infallible, mem::MaybeUninit, task::Poll};

use dope::{
    core::{
        driver::life::{creation::Attempt, recovery},
        platform::signal::Source,
    },
    runtime::{executor::session::Completion, signal::Signal},
};
use dope_fiber::{
    BlockOn,
    abi::{
        Ready, poll_fn,
        race::{Either, Race},
    },
    task::batch,
};
use o3::mem::slot;

pub mod common;
use common::runtime::{Empty, Setup, profile::Tasks, terminate};

fn main() {
    for phase in 0..4 {
        let mut retained = MaybeUninit::uninit();
        Source::scope(slot::Vacant::new(&mut retained), |source| {
            let mut signal = Signal::new(source);
            for generation in 0..2 {
                let mut attempt = Attempt::new();
                let executor = Setup::for_profile::<Tasks>()
                    .try_executor(&mut attempt)
                    .unwrap_or_else(|error| panic!("executor: {:?}", error.kind()))
                    .shutdown(&mut signal)
                    .expect("signal registration");
                executor.run(|executor| {
                    executor
                        .enter_with(
                            |mut scope| {
                                let mut finish = Completion::new();
                                if generation == 0 && phase != 0 {
                                    scope
                                        .with_app(&mut finish, Empty, |mut app| {
                                            terminate();
                                            match phase {
                                                1 => {
                                                    let _request =
                                                        app.run().expect("ordinary shutdown");
                                                }
                                                2 => {}
                                                3 => {
                                                    app.block_on(Ready::new(()))
                                                        .expect("completed root");
                                                }
                                                _ => unreachable!(),
                                            }
                                        })
                                        .expect("earlier application");
                                }
                                scope
                                    .with_app(&mut finish, Empty, |app| {
                                        app.with_signal(|mut app, mut signal| {
                                            let root = dope_fiber::fiber!('_ => async move {
                                                if generation == 0 && phase == 0 {
                                                    assert!(matches!(
                                                        Race::new(&mut signal, Ready::new(())).await,
                                                        Either::Right(())
                                                    ));
                                                    terminate();
                                                }
                                                let mut pool = batch::Pool::<1>::acquire()
                                                    .await
                                                    .expect("signal group");
                                                batch::Join::from_array(&mut pool, [&mut signal])
                                                    .await
                                                    .expect("signal completion")
                                                    .next()
                                                    .expect("signal output")
                                                    .expect("termination read");
                                                let mut yielded = false;
                                                poll_fn(|context| {
                                                    if yielded {
                                                        Poll::Ready(())
                                                    } else {
                                                        yielded = true;
                                                        context.wake();
                                                        Poll::Pending
                                                    }
                                                }).await;
                                                42
                                            });
                                            app.block_on(root).expect("cleanup drive")
                                        })
                                    })
                                    .map(|value| assert_eq!(value, 42))
                                    .expect("signal application");
                            },
                            recovery::Policy::new(
                                |event| panic!("reclaimed: {event:?}"),
                                common::recover,
                            ),
                        )
                        .unwrap_or_else(|error| panic!("generation: {:?}", error.cause()));
                });
                assert!(matches!(signal.result(), Some(Ok(()))));
            }
            Ok::<_, Infallible>(())
        })
        .expect("signal scope");
    }
}

use o3::depot::capacity::Count;
use std::{num, pin};

use dope::core::{
    driver::{
        life::recovery,
        route::{Epoch, KeyTag, indices::SlotIndex},
        schedule::ready::{
            self,
            task::owner::{Capacity, Generation, Lane, Owner},
        },
    },
    io,
};
use dope_fiber::{
    abi,
    context::wake::{self, RootWaker},
    task::storage::{
        fixed::schedule::{
            Pool,
            queue::{self, Owned},
        },
        scope::Scope,
    },
};
use dope_test::scope;

use crate::common::{
    runtime::{Setup, profile::Tasks},
    storage::policies::owner,
};

#[derive(Debug)]
pub enum Error {
    Ready(ready::Capacity),
    Queue(queue::PrepareError),
}

pub fn with_plane(
    run: impl for<'s, 'd> FnOnce(
        Scope<'s, pin::Pin<&'s mut Pool<'d, abi::Ready<()>, (), u8, Owned<'d, owner::Tasks>>>>,
        wake::RootWaker<'d>,
        Owner<owner::Tasks>,
    ),
    recover: impl recovery::Recover,
    finish: impl for<'owner> FnOnce(Result<Result<(), Error>, scope::Error<'owner, Result<(), Error>>>),
) -> Vec<io::Reclaimed> {
    let mut reports = Vec::new();
    scope::Driver::new(Setup::for_profile::<Tasks>().config()).with_driver(
        |driver| {
            let view = driver.driver_ref();
            let targets = view.targets::<KeyTag<0>>();
            let ready = view
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .map_err(Error::Ready)?;
            let parent = RootWaker::from(ready.target());
            let plane = Pool::<'_, abi::Ready<()>, (), u8, Owned<'_, owner::Tasks>>::try_owned(
                Count::new(1),
                Capacity::new(num::NonZero::<u32>::MIN),
            )
            .map_err(Error::Queue)?;
            let mut plane = pin::pin!(plane);
            Scope::with(plane.as_mut(), |plane| {
                let owner = Owner::from(owner::Stamp(
                    Lane::from(SlotIndex::ZERO),
                    Generation::from_u32(0),
                ));
                run(plane, parent, owner);
            });
            Ok(())
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    reports
}

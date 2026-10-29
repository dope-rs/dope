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
        fixed::{
            quota::Physical,
            schedule::{
                Plane, Pool,
                queue::{self, Owned},
            },
            tree::Leaf,
        },
        scope::Scope,
    },
};
use dope_test::scope;
use o3::alloc;

use crate::common::{
    runtime::{Setup, profile::Tasks},
    storage::policies::owner,
};

#[derive(Debug)]
pub enum Error {
    Ready(ready::Capacity),
    Pool(alloc::Error),
    Queue(queue::PrepareError),
}

pub fn with_plane(
    run: impl for<'s, 'd> FnOnce(
        Scope<
            's,
            pin::Pin<
                &'s mut Plane<
                    'd,
                    Leaf<Pool<'d, abi::Ready<()>, 1, (), u8>>,
                    Physical,
                    Owned<'d, owner::Tasks>,
                >,
            >,
        >,
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
            let pool = Pool::<'_, abi::Ready<()>, 1, (), u8>::try_new().map_err(Error::Pool)?;
            let plane = Plane::<'_, _, Physical, Owned<'_, owner::Tasks>>::try_owned(
                Leaf::new(pool),
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

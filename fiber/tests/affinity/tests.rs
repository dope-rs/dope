use dope_fiber::{
    abi::{Ready, batch::Batch},
    context::Waker,
    task::{
        Scheduler,
        storage::{Id, RoutedId, Slab, fixed},
    },
};
use dope_test::checks::affinities::Affinity;

const _: fn() = || {
    Affinity::<Waker<'static>>::not_send::<_>();
    Affinity::<Waker<'static>>::not_sync::<_>();
    Affinity::<Id<'static>>::not_send::<_>();
    Affinity::<Id<'static>>::not_sync::<_>();
    Affinity::<RoutedId<'static, (), 0, ()>>::not_send::<_>();
    Affinity::<RoutedId<'static, (), 0, ()>>::not_sync::<_>();
    Affinity::<Slab<'static, Ready<()>>>::not_send::<_>();
    Affinity::<Slab<'static, Ready<()>>>::not_sync::<_>();
    Affinity::<Scheduler<'static, Ready<()>>>::not_send::<_>();
    Affinity::<Scheduler<'static, Ready<()>>>::not_sync::<_>();
    Affinity::<fixed::Slab<'static, Ready<()>, 1>>::not_send::<_>();
    Affinity::<fixed::Slab<'static, Ready<()>, 1>>::not_sync::<_>();
    Affinity::<fixed::VacantEntry<'static, 'static, Ready<()>, 1>>::not_send::<_>();
    Affinity::<fixed::VacantEntry<'static, 'static, Ready<()>, 1>>::not_sync::<_>();
};

const _: fn() = || {
    Affinity::<fixed::Slab<'static, Ready<()>, 1>>::not_unpin::<_>();
    Affinity::<Batch<'_, '_, Ready<()>, (), 1>>::not_unpin::<_>();
};

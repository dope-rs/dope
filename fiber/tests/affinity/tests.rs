use dope_fiber::abi::batch::Batch;
use dope_fiber::abi::ready::Ready;
use dope_fiber::slab::{
    ErasedTaskId, FixedSlab, FixedSlabVacantEntry, Slab, SlabVacantEntry, TaskId,
};
use dope_fiber::task::queue::TaskQueue;
use dope_fiber::task::{TaskContext, Waker};
use dope_test::{not_send, not_sync, not_unpin};

const _: fn() = || {
    not_send::<TaskQueue, _>();
    not_sync::<TaskQueue, _>();
    not_send::<TaskContext, _>();
    not_sync::<TaskContext, _>();
    not_send::<Waker<'static>, _>();
    not_sync::<Waker<'static>, _>();
    not_send::<TaskId, _>();
    not_sync::<TaskId, _>();
    not_send::<ErasedTaskId, _>();
    not_sync::<ErasedTaskId, _>();
    not_send::<Slab<'static, Ready<()>>, _>();
    not_sync::<Slab<'static, Ready<()>>, _>();
    not_send::<SlabVacantEntry<'static, Ready<()>>, _>();
    not_sync::<SlabVacantEntry<'static, Ready<()>>, _>();
    not_send::<FixedSlab<'static, Ready<()>, 1>, _>();
    not_sync::<FixedSlab<'static, Ready<()>, 1>, _>();
    not_send::<FixedSlabVacantEntry<'static, Ready<()>, 1>, _>();
    not_sync::<FixedSlabVacantEntry<'static, Ready<()>, 1>, _>();
};

const _: fn() = || {
    not_unpin::<FixedSlab<'static, Ready<()>, 1>, _>();
    not_unpin::<Batch<Ready<()>, (), 1>, _>();
};

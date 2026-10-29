use settings::demands::slots;

use crate::driver::{route::table, settings};

const _: () = {
    assert!(std::mem::size_of::<settings::Progress>() == std::mem::size_of::<bool>());
    assert!(std::mem::align_of::<settings::Progress>() == std::mem::align_of::<bool>());
    let queues = settings::QueueLayout::fixed::<
        { settings::MAX_SUBMISSION_CAPACITY },
        { settings::MAX_COMPLETION_CAPACITY },
    >();
    assert!(queues.submission().get() == settings::MAX_SUBMISSION_CAPACITY);
    assert!(queues.completions() == settings::MAX_COMPLETION_CAPACITY);
    assert!(std::mem::size_of::<settings::Submission>() == std::mem::size_of::<u32>());
    assert!(std::mem::align_of::<settings::Submission>() == std::mem::align_of::<u32>());
    assert!(std::mem::size_of::<settings::QueueLayout>() == std::mem::size_of::<[u32; 2]>());
    assert!(slots::Slots::MAX.count().get() == u32::MAX as usize);
    assert!(std::mem::size_of::<slots::Slots>() == std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<settings::Schedule>() == std::mem::size_of::<[u32; 2]>());
    assert!(std::mem::size_of::<settings::Buffers>() == std::mem::size_of::<u16>());
    assert!(std::mem::align_of::<settings::Buffers>() == std::mem::align_of::<u16>());
    assert!(std::mem::size_of::<settings::Receive>() == std::mem::size_of::<u64>());
    assert!(
        std::mem::size_of::<settings::FileSlots>() == std::mem::size_of::<[table::Capacity; 2]>()
    );
};

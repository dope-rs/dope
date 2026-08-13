pub(super) mod event;
pub(super) mod lifecycle;
pub(super) mod read;
pub(super) mod receive;
pub(super) mod runtime;
pub(super) mod submit;
pub(super) mod table;
pub(super) mod write;

const _: () = assert!(
    std::mem::size_of::<Option<std::os::fd::OwnedFd>>()
        <= std::mem::size_of::<Option<std::os::fd::RawFd>>()
);
const _: () = assert!(std::mem::size_of::<usize>() == std::mem::size_of::<u64>());

mod failure;
mod kernel;
mod mapping;

use dope_sys::{
    engine::ring::create,
    io::reactor::{self, ready::store},
    ring::map::Map,
};
pub(crate) use failure::Failure;
pub(crate) use kernel::Kernel;
pub(crate) use mapping::Mapping;

use crate::driver::{
    result,
    result::{CreateError, IoOperation},
    storage,
};

impl From<store::Create> for CreateError {
    fn from(error: store::Create) -> Self {
        match error {
            store::Create::Buffer(error) => CreateError::Buffer(error),
            store::Create::Allocation(error) => CreateError::Allocation(error),
            store::Create::Queue(error) => CreateError::Queue(error),
            store::Create::Reactor(reactor::Create::Open(error)) => {
                CreateError::kernel(IoOperation::ReactorCreate, Kernel::from(error))
            }
            store::Create::Reactor(reactor::Create::Cloexec(error)) => {
                CreateError::ReactorCloexec(result::Cloexec::Set(error))
            }
        }
    }
}

impl<S, M, P, C, R> From<create::Failure<S, M, P, C, R>> for CreateError
where
    Kernel: From<S>,
    Mapping: From<Map<M>>,
    Self: From<P> + From<R>,
    crate::Resources: From<C>,
{
    fn from(error: create::Failure<S, M, P, C, R>) -> Self {
        match error {
            create::Failure::Allocation(error) => CreateError::Allocation(error),
            create::Failure::Setup(error) => {
                CreateError::kernel(IoOperation::ReactorCreate, Kernel::from(error))
            }
            create::Failure::Layout(error) => CreateError::RingLayout(error),
            create::Failure::Mapping(error) => CreateError::Mapping(Mapping::from(error)),
            create::Failure::Primary(error) => Self::from(error),
            create::Failure::Cleanup(state) => CreateError::Cleanup(storage::Cleanup::new(state)),
            create::Failure::Release(state) => Self::from(state),
        }
    }
}

impl<R, S, C> From<Result<(R, S), C>> for CreateError
where
    ((IoOperation, result::Kernel), crate::RemoveSlot): From<(R, S)>,
    crate::Released: From<C>,
{
    fn from(error: Result<(R, S), C>) -> Self {
        match error.map(Into::into).map(storage::Removal::native) {
            Ok(primary) => CreateError::Removal(primary),
            Err(state) => CreateError::Release(storage::Release::new(state)),
        }
    }
}

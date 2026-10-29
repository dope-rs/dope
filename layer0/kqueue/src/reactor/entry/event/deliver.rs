use dope_sys::{
    io::reactor::ready::delivery::entry,
    table::{self, Files as _, token::host::Flight as _},
};

use crate::reactor::entry::{
    Key, Slots,
    create::Create,
    event::{Failure, State},
};

pub(super) fn try_deliver<'q, 'd, C: entry::Socket<'q, 'd, B>, B>(
    state: State<C, B>,
    bound: C::Bound,
    key: Key<C>,
    files: &mut C::Files,
    driver: C::Driver,
) -> Delivery<'q, 'd, C, B, C::Output> {
    Ok(match state {
        State::AcceptSuccess { accepted, flow } => {
            let (bound, (accepted, flow)) =
                try_bound::<C, B, C::Accept, _>(bound, (accepted, flow), |(accepted, flow)| {
                    State::AcceptSuccess { accepted, flow }
                })?;
            C::accepted(bound, accepted, flow)
        }
        State::AcceptFailure { failure, flow } => {
            let (bound, (failure, flow)) =
                try_bound::<C, B, C::Accept, _>(bound, (failure, flow), |(failure, flow)| {
                    State::AcceptFailure { failure, flow }
                })?;
            C::accept_error(bound, failure, flow)
        }
        State::RecvData { flow, buffer } => {
            C::received(bound, driver, key, flow, buffer).map_err(|rejected| {
                let (buffer, fault) = rejected.into_parts();
                Failure::Completion {
                    state: State::RecvData { flow, buffer },
                    fault,
                }
            })?
        }
        State::RecvControl { result, flow } => {
            let (bound, (result, flow)) =
                try_bound::<C, B, C::Recv, _>(bound, (result, flow), |(result, flow)| {
                    State::RecvControl { result, flow }
                })?;
            C::control(bound, result, flow)
        }
        State::Send { result } => {
            let (bound, result) =
                try_bound::<C, B, C::Send, _>(bound, result, |result| State::Send { result })?;
            C::sent(bound, result)
        }
        State::Connect { result } => {
            let (bound, result) = try_bound::<C, B, C::Connect, _>(bound, result, |result| {
                State::Connect { result }
            })?;
            C::connected(bound, result)
        }
        State::Open { result } => {
            let (bound, result) =
                try_bound::<C, B, C::Open, _>(bound, result, |result| State::Open { result })?;
            C::opened(bound, result)
        }
        State::Write { result } => {
            let (bound, result) =
                try_bound::<C, B, C::Write, _>(bound, result, |result| State::Write { result })?;
            C::written(bound, result)
        }
        State::Sync { result } => {
            let (bound, result) =
                try_bound::<C, B, C::Sync, _>(bound, result, |result| State::Sync { result })?;
            C::synced(bound, result)
        }
        State::Create { outcome } => {
            let (bound, Create { slot, result }) =
                try_bound::<C, B, C::Socket, _>(bound, outcome, |outcome| State::Create {
                    outcome,
                })?;
            let target = C::Target::from(&bound);
            let creation = match Create::<C>::try_creation::<B>(slot, key, driver, target) {
                Ok(creation) => creation,
                Err(rejected) => {
                    let (creation, fault) = rejected.into_parts();
                    if let Some(creation) = creation {
                        C::fail_create(driver, creation);
                    }
                    bound.finish();
                    return Err(Failure::Completion {
                        state: State::Create {
                            outcome: Create { slot, result },
                        },
                        fault,
                    });
                }
            };
            bound.finish();
            match result {
                Ok(fd) => {
                    let creation = try_install::<C, B>(files, fd, creation, driver, target, key)
                        .map_err(|fault| Failure::Retired {
                            report: entry::Retired::Create {
                                key,
                                slot,
                                result: Ok(()),
                                close: None,
                            },
                            fault,
                        })?;
                    C::created(driver, target, creation)
                }
                Err(error) => C::failed(driver, target, creation, error),
            }
        }
    })
}

fn try_bound<'q, 'd, C, B, T, V>(
    bound: C::Bound,
    value: V,
    state: impl FnOnce(V) -> State<C, B>,
) -> Delivery<'q, 'd, C, B, (T, V)>
where
    C: entry::Effects<'q, 'd, B>,
    T: TryFrom<C::Bound, Error = C::Mismatch>,
{
    match T::try_from(bound) {
        Ok(bound) => Ok((bound, value)),
        Err(cause) => Err(Failure::Operation {
            state: state(value),
            cause,
        }),
    }
}

fn try_install<'q, 'd, C: entry::Socket<'q, 'd, B>, B>(
    files: &mut C::Files,
    fd: C::Handle,
    creation: C::Creation,
    driver: C::Driver,
    target: C::Target,
    key: Key<C>,
) -> Result<C::Creation, C::Error> {
    let slot = *creation.as_ref();
    match files.install(slot.into(), fd) {
        Ok(()) => Ok(creation),
        Err(cause) => {
            C::fail_create(driver, creation);
            let owned: <Slots<C> as table::raw::Slots>::Owned = slot.into();
            Err(C::bad_install(key, owned.into(), target, cause))
        }
    }
}

/// Delivery preserves the original entry state for either operation or completion rejection.
type Delivery<'q, 'd, C, B, T> = Result<
    T,
    Failure<
        C,
        B,
        <C as entry::Effects<'q, 'd, B>>::Mismatch,
        <C as entry::Effects<'q, 'd, B>>::Error,
    >,
>;

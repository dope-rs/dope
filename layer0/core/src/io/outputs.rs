use std::{mem, os::fd};

const _: () = assert!(mem::size_of::<Result<(), Errno>>() == mem::size_of::<i32>());

use crate::{
    Errno, driver,
    driver::{route, route::kind},
    io::{
        Event, event,
        event::{
            batch::Flow,
            file::{open, sync, write},
            net::{accept, connect, recv, send, socket},
        },
        fd::handles::{self, Accepted, Created},
        recv::Lease,
        socket::Create,
        transfer,
    },
};

pub(crate) const fn shutdown<'d>() -> Event<'d> {
    Event(event::Kind::Shutdown)
}

pub(crate) fn accepted<'d>(
    token: route::Erased<'d, kind::Accept>,
    accepted: Accepted,
    flow: Flow,
    driver: driver::Reference<'d>,
) -> Event<'d> {
    let outcome = match accepted.bind(driver) {
        handles::Admission::Ready(accepted) => accept::Outcome::Accepted(accepted),
        handles::Admission::Retired(retired) => accept::Outcome::Failed(retired.into()),
    };
    Event(event::Kind::Accept(accept::Completion::new(
        token, flow, outcome,
    )))
}

pub(crate) fn accept_error<'d>(
    token: route::Erased<'d, kind::Accept>,
    failure: impl Into<accept::Failure>,
    flow: Flow,
) -> Event<'d> {
    Event(event::Kind::Accept(accept::Completion::new(
        token,
        flow,
        accept::Outcome::Failed(failure.into()),
    )))
}

pub(crate) fn opened<'d>(token: route::Erased<'d, kind::Open>, opened: fd::OwnedFd) -> Event<'d> {
    Event(event::Kind::Open(open::Completion::new(
        token,
        open::Outcome::Opened(opened),
    )))
}

pub(crate) fn open_failed<'d>(token: route::Erased<'d, kind::Open>, errno: Errno) -> Event<'d> {
    Event(event::Kind::Open(open::Completion::new(
        token,
        open::Outcome::Failed(errno),
    )))
}

pub(crate) fn socket<'d>(
    token: route::Erased<'d, kind::Socket>,
    created: Created<'d>,
    driver: driver::Reference<'d>,
) -> Event<'d> {
    Event(event::Kind::Socket(socket::Completion::new(
        token,
        socket::Outcome::Created(created.bind(driver)),
    )))
}

pub(crate) fn socket_gone<'d>(token: route::Erased<'d, kind::Socket>) -> Event<'d> {
    Event(event::Kind::Socket(socket::Completion::new(
        token,
        socket::Outcome::Failed(socket::Failure::Retired),
    )))
}

pub(crate) fn socket_error<'d>(token: route::Erased<'d, kind::Socket>, error: Create) -> Event<'d> {
    Event(event::Kind::Socket(socket::Completion::new(
        token,
        socket::Outcome::Failed(socket::Failure::Create(error)),
    )))
}

pub(crate) fn selected<'d>(
    token: route::Erased<'d, kind::Recv>,
    flow: Flow,
    buffer: Lease<'d>,
) -> Event<'d> {
    Event(event::Kind::Recv(recv::Completion::new(
        token,
        flow,
        recv::Outcome::Data(buffer),
    )))
}

pub(crate) fn sent<'d>(
    token: route::Erased<'d, kind::Send>,
    outcome: Result<transfer::Len, Errno>,
) -> Event<'d> {
    Event(event::Kind::Send(send::Completion::new(token, outcome)))
}

pub(crate) fn connected<'d>(
    token: route::Erased<'d, kind::Connect>,
    outcome: Result<(), Errno>,
) -> Event<'d> {
    Event(event::Kind::Connect(connect::Completion::new(
        token, outcome,
    )))
}

pub(crate) fn written<'d>(
    token: route::Erased<'d, kind::Write>,
    outcome: Result<transfer::Len, Errno>,
) -> Event<'d> {
    Event(event::Kind::Write(write::Completion::new(token, outcome)))
}

pub(crate) fn synced<'d>(
    token: route::Erased<'d, kind::Sync>,
    outcome: Result<(), Errno>,
) -> Event<'d> {
    Event(event::Kind::Sync(sync::Completion::new(token, outcome)))
}

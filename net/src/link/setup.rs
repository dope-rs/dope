use std::{io, mem, process};

use dope_core::{
    driver,
    driver::{ops::Control as _, schedule::ready},
    io::{
        event::{connect, tuning},
        fd::handles,
        socket::{establishment, option},
    },
};

use crate::link::pool;

pub(crate) struct Setup<'d> {
    state: State<'d>,
}

enum State<'d> {
    Poisoned,
    Creating(handles::CreatingSocket<'d>),
    CreatingTargeted(handles::CreatingSocket<'d>),
    Idle(handles::Descriptor<'d>),
    Missing(handles::Descriptor<'d>),
    Tuning(establishment::TuningPending<'d>),
    TuningCancelled(establishment::TuningPending<'d, establishment::Cancelled>),
    Connecting(establishment::ConnectionPending<'d>),
    ConnectingCancelled(establishment::ConnectionPending<'d, establishment::Cancelled>),
    Done(handles::Descriptor<'d>),
}

pub(crate) enum Authority<'d> {
    Creating(handles::CreatingSocket<'d>),
    Live(handles::Descriptor<'d>),
}

pub(crate) enum Completion<T> {
    Failed(io::Error),
    Idle,
    Done,
    Connected(T),
}

pub(crate) enum Submission {
    Missing,
    Pending,
    Failed(driver::SubmitError),
}

pub(crate) enum Cancellation {
    Idle,
    Pending,
    Blocked,
}

impl<'d> Setup<'d> {
    pub(crate) fn creating(socket: handles::CreatingSocket<'d>) -> Self {
        Self {
            state: State::Creating(socket),
        }
    }

    pub(crate) fn creating_targeted<const ID: u8>(
        socket: handles::CreatingSocket<'d>,
        _stored: pool::StoredAddress<'d, ID>,
    ) -> Self {
        Self {
            state: State::CreatingTargeted(socket),
        }
    }

    pub(crate) fn tuning(establishment: establishment::TuningPending<'d>) -> Self {
        Self {
            state: State::Tuning(establishment),
        }
    }

    pub(crate) fn done(fd: handles::Descriptor<'d>) -> Self {
        Self {
            state: State::Done(fd),
        }
    }

    pub(crate) fn idle(fd: handles::Descriptor<'d>) -> Self {
        Self {
            state: State::Idle(fd),
        }
    }

    pub(crate) fn complete_tuning(&mut self, completion: tuning::Completion) -> bool {
        let state = mem::replace(&mut self.state, State::Poisoned);
        match state {
            State::Tuning(establishment) => {
                let (fd, outcome) = match establishment.complete_tuning(completion) {
                    Ok(completed) => completed,
                    Err((establishment, _)) => {
                        self.state = State::Tuning(establishment);
                        return false;
                    }
                };
                match outcome {
                    tuning::Outcome::Applied => {
                        self.state = State::Done(fd);
                        true
                    }
                    tuning::Outcome::Failed(_) => {
                        self.state = State::Idle(fd);
                        false
                    }
                }
            }
            State::TuningCancelled(establishment) => {
                let (fd, outcome) = match establishment.complete_tuning(completion) {
                    Ok(completed) => completed,
                    Err((establishment, _)) => {
                        self.state = State::TuningCancelled(establishment);
                        return false;
                    }
                };
                match outcome {
                    tuning::Outcome::Applied => {
                        self.state = State::Done(fd);
                        true
                    }
                    tuning::Outcome::Failed(_) => {
                        self.state = State::Idle(fd);
                        false
                    }
                }
            }
            state => {
                self.state = state;
                false
            }
        }
    }

    pub(in crate::link) fn created(
        &mut self,
        created: handles::CreatedSlot<'d>,
        options: Option<option::StreamOptions>,
        submit: impl FnOnce(
            handles::Descriptor<'d>,
            option::StreamOptions,
        )
            -> Result<establishment::ConnectionPending<'d>, handles::Descriptor<'d>>,
    ) -> Submission {
        let state = mem::replace(&mut self.state, State::Poisoned);
        let (socket, targeted) = match state {
            State::Creating(socket) => (socket, false),
            State::CreatingTargeted(socket) => (socket, true),
            state => {
                self.state = state;
                return Submission::Pending;
            }
        };
        let fd = match created.activate(socket) {
            Ok(fd) => fd,
            Err((socket, created)) => {
                self.state = if targeted {
                    State::CreatingTargeted(socket)
                } else {
                    State::Creating(socket)
                };
                drop(created);
                return Submission::Pending;
            }
        };
        if !targeted {
            self.state = State::Missing(fd);
            return Submission::Missing;
        }
        let Some(options) = options else {
            self.state = State::Idle(fd);
            return Submission::Missing;
        };
        match submit(fd, options) {
            Ok(pending) => {
                self.state = State::Connecting(pending);
                Submission::Pending
            }
            Err(fd) => {
                self.state = State::Idle(fd);
                Submission::Failed(driver::SubmitError)
            }
        }
    }

    pub(crate) fn complete<T>(
        &mut self,
        completion: connect::Completion,
        connected: impl FnOnce() -> T,
    ) -> Completion<T> {
        let state = mem::replace(&mut self.state, State::Poisoned);
        match state {
            State::Connecting(pending) => {
                let (fd, outcome) = match pending.complete_connection(completion) {
                    Ok(completed) => completed,
                    Err((pending, _)) => {
                        self.state = State::Connecting(pending);
                        return Completion::Idle;
                    }
                };
                match outcome {
                    connect::Outcome::Connected => {
                        self.state = State::Done(fd);
                        Completion::Connected(connected())
                    }
                    connect::Outcome::Failed(error) => {
                        self.state = State::Idle(fd);
                        Completion::Failed(error)
                    }
                }
            }
            State::ConnectingCancelled(pending) => {
                let (fd, outcome) = match pending.complete_connection(completion) {
                    Ok(completed) => completed,
                    Err((pending, _)) => {
                        self.state = State::ConnectingCancelled(pending);
                        return Completion::Idle;
                    }
                };
                match outcome {
                    connect::Outcome::Connected => {
                        self.state = State::Done(fd);
                        Completion::Connected(connected())
                    }
                    connect::Outcome::Failed(error) => {
                        self.state = State::Idle(fd);
                        Completion::Failed(error)
                    }
                }
            }
            State::Done(fd) => {
                self.state = State::Done(fd);
                Completion::Done
            }
            state => {
                self.state = state;
                Completion::Idle
            }
        }
    }

    pub(crate) fn fd(&self) -> Option<&handles::Descriptor<'d>> {
        match &self.state {
            State::Poisoned => None,
            State::Creating(_) | State::CreatingTargeted(_) => None,
            State::Idle(fd) | State::Missing(fd) | State::Done(fd) => Some(fd),
            State::Tuning(establishment) => Some(establishment.fd()),
            State::TuningCancelled(establishment) => Some(establishment.fd()),
            State::Connecting(pending) => Some(pending.fd()),
            State::ConnectingCancelled(pending) => Some(pending.fd()),
        }
    }

    pub(crate) fn driver(&self) -> driver::Reference<'d> {
        match &self.state {
            State::Poisoned => process::abort(),
            State::Creating(socket) | State::CreatingTargeted(socket) => socket.driver(),
            State::Idle(fd) | State::Missing(fd) | State::Done(fd) => fd.driver(),
            State::Tuning(establishment) => establishment.fd().driver(),
            State::TuningCancelled(establishment) => establishment.fd().driver(),
            State::Connecting(pending) => pending.fd().driver(),
            State::ConnectingCancelled(pending) => pending.fd().driver(),
        }
    }

    pub(crate) fn ready_handle(&self) -> ready::Handle<'d> {
        match &self.state {
            State::Poisoned => process::abort(),
            State::Creating(socket) | State::CreatingTargeted(socket) => socket.ready_handle(),
            State::Idle(fd) | State::Missing(fd) | State::Done(fd) => fd.ready_handle(),
            State::Tuning(establishment) => establishment.fd().ready_handle(),
            State::TuningCancelled(establishment) => establishment.fd().ready_handle(),
            State::Connecting(pending) => pending.fd().ready_handle(),
            State::ConnectingCancelled(pending) => pending.fd().ready_handle(),
        }
    }

    pub(crate) fn into_authority(self) -> Authority<'d> {
        match self.state {
            State::Poisoned => process::abort(),
            State::Creating(socket) | State::CreatingTargeted(socket) => {
                Authority::Creating(socket)
            }
            State::Idle(fd) | State::Missing(fd) | State::Done(fd) => Authority::Live(fd),
            State::Tuning(_)
            | State::TuningCancelled(_)
            | State::Connecting(_)
            | State::ConnectingCancelled(_) => process::abort(),
        }
    }

    pub(crate) fn is_connecting(&self) -> bool {
        matches!(
            self.state,
            State::Connecting(_) | State::ConnectingCancelled(_)
        )
    }

    pub(crate) fn is_tuning(&self) -> bool {
        matches!(self.state, State::Tuning(_) | State::TuningCancelled(_))
    }

    pub(crate) fn cancel(&mut self, driver: &mut driver::Context<'_, 'd>) -> Cancellation {
        let state = mem::replace(&mut self.state, State::Poisoned);
        match state {
            State::Tuning(pending) => match driver.cancel_tuning(pending) {
                Ok(pending) => {
                    self.state = State::TuningCancelled(pending);
                    Cancellation::Pending
                }
                Err((pending, _)) => {
                    self.state = State::Tuning(pending);
                    Cancellation::Blocked
                }
            },
            State::Connecting(pending) => match driver.cancel_connection(pending) {
                Ok(pending) => {
                    self.state = State::ConnectingCancelled(pending);
                    Cancellation::Pending
                }
                Err((pending, _)) => {
                    self.state = State::Connecting(pending);
                    Cancellation::Blocked
                }
            },
            State::TuningCancelled(pending) => {
                self.state = State::TuningCancelled(pending);
                Cancellation::Pending
            }
            State::ConnectingCancelled(pending) => {
                self.state = State::ConnectingCancelled(pending);
                Cancellation::Pending
            }
            state => {
                self.state = state;
                Cancellation::Idle
            }
        }
    }

    pub(crate) fn is_done(&self) -> bool {
        matches!(self.state, State::Done(_))
    }
}

const _: () = assert!(
    mem::size_of::<State<'static>>()
        <= mem::size_of::<(establishment::ConnectionPending<'static>, usize)>()
);
const _: () = assert!(mem::size_of::<Setup<'static>>() == mem::size_of::<State<'static>>());

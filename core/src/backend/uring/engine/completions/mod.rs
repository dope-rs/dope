use std::{mem, os::fd, process};

use io_uring::cqueue;

use crate::{
    backend::{
        fixed,
        uring::{
            self,
            engine::{controls, lifecycle, tuning},
        },
    },
    driver::{
        self, flight,
        route::{self, kind},
    },
    io::{self, event::open, fd::handles},
};

mod sealed;
pub(super) use sealed::{Decoder, Opened};

#[repr(transparent)]
pub(in crate::backend::uring) struct Cqe(cqueue::Entry);

enum UserData<'q, 'd> {
    Framework(route::Token),
    Flight(flight::Completion<'q, 'd>),
    Empty,
}

const _: () = {
    assert!(mem::size_of::<Cqe>() == mem::size_of::<cqueue::Entry>());
};

impl Cqe {
    pub(in crate::backend::uring) const fn new(entry: cqueue::Entry) -> Self {
        Self(entry)
    }

    fn decode<'q, 'd>(self, drain: &'q flight::Drain<'q, 'd>) -> (Self, UserData<'q, 'd>) {
        let data = Decoder::new(self.0.user_data()).decode(drain);
        (self, data)
    }

    fn result(&self) -> i32 {
        self.0.result()
    }

    fn flags(&self) -> u32 {
        self.0.flags()
    }

    fn into_retired(self, retire: controls::Retire) -> Closed {
        let result = self.result();
        let slot = handles::FixedSlot::from_index(retire.slot());
        let work = lifecycle::CloseWork::retired(slot);
        Closed { work, result }
    }

    fn into_opened(self, token: route::Token) -> Result<fd::OwnedFd, i32> {
        debug_assert_eq!(token.kind(), kind::OPEN);
        Opened::new(self.result()).into_fd()
    }
}

#[must_use = "resolved completion may own a resource that must be delivered or reclaimed"]
pub(in crate::backend::uring) enum Disposition {
    Consumed(Option<uring::ffi::Buffer>),
    Public(io::Completion),
    Closed(Closed),
}

#[must_use = "a fixed-file close completion must settle or restore its one-shot work"]
pub(in crate::backend::uring) struct Closed {
    work: lifecycle::CloseWork,
    result: i32,
}

impl Closed {
    pub(in crate::backend::uring) fn result(&self) -> i32 {
        self.result
    }

    pub(in crate::backend::uring) fn work(&self) -> &lifecycle::CloseWork {
        &self.work
    }

    pub(in crate::backend::uring) fn into_work(self) -> lifecycle::CloseWork {
        self.work
    }

    pub(in crate::backend::uring) fn settle(
        self,
        fixed_slots: &mut fixed::Slots,
        driver: driver::Reference<'_>,
    ) {
        let work = match self.work.into_retire() {
            Ok(work) => {
                fixed_slots.release_slot(work.into_retirement(driver).into_slot());
                return;
            }
            Err(work) => work,
        };
        let slot = work.slot();
        if let Some(retired) = driver.outbound().complete_outbound_close(slot) {
            let slots = driver.outbound().take_retired_slots(retired);
            fixed_slots.release(slots);
        }
    }
}

pub(in crate::backend::uring) struct Resolver<'a, 'q, 'd> {
    provided: &'a uring::ffi::ProvidedRing,
    tuning: &'a mut tuning::Table,
    fixed_slots: &'a mut fixed::Slots,
    drain: &'q flight::Drain<'q, 'd>,
}

impl<'a, 'q, 'd> Resolver<'a, 'q, 'd> {
    pub(in crate::backend::uring) fn new(
        provided: &'a uring::ffi::ProvidedRing,
        tuning: &'a mut tuning::Table,
        fixed_slots: &'a mut fixed::Slots,
        drain: &'q flight::Drain<'q, 'd>,
    ) -> Self {
        Self {
            provided,
            tuning,
            fixed_slots,
            drain,
        }
    }

    pub(in crate::backend::uring) fn resolve(mut self, cqe: Cqe) -> Disposition {
        let (cqe, user_data) = cqe.decode(self.drain);
        let result = cqe.result();
        let flags = cqe.flags();
        let buffer =
            cqueue::buffer_select(flags).map(|bid| self.provided.buffer_from_completion(bid));
        let more = cqueue::more(flags);

        if matches!(
            &user_data,
            UserData::Framework(token) if *token == route::SHUTDOWN
        ) {
            return Self::public(route::SHUTDOWN, result, more, buffer);
        }
        if let UserData::Framework(token) = &user_data
            && let Ok(control) = controls::Decoded::try_from(*token)
        {
            let completed = match control {
                controls::Decoded::Tuning(transaction) => self.tuning.complete(transaction, result),
                controls::Decoded::ClosePrep => return Disposition::Consumed(buffer),
                controls::Decoded::Close(close) => {
                    if buffer.is_some() {
                        process::abort();
                    }
                    return Disposition::Closed(Closed {
                        work: lifecycle::CloseWork::completed_close(close),
                        result,
                    });
                }
                controls::Decoded::Retire(retire) => {
                    if buffer.is_some() {
                        process::abort();
                    }
                    return Disposition::Closed(cqe.into_retired(retire));
                }
            };
            return match completed {
                Some((target, result)) => Self::public(target, result, false, buffer),
                None => Disposition::Consumed(buffer),
            };
        }
        if matches!(&user_data, UserData::Framework(_)) {
            return Disposition::Consumed(buffer);
        }
        let UserData::Flight(completion) = user_data else {
            return Disposition::Consumed(buffer);
        };
        let Some(token) = completion.resolve(more) else {
            return Disposition::Consumed(buffer);
        };
        let driver = self.drain.driver();
        let op_kind = token.kind();
        if op_kind == kind::SOCKET {
            let Some(slot) = driver.files().outbound_slot_for_target(token) else {
                return Disposition::Consumed(buffer);
            };
            let completion = if result >= 0 {
                io::Completion::socket_created(token, slot)
            } else {
                if let Some(retired) = driver.outbound().complete_outbound_create_failure(slot) {
                    self.release_slots(retired);
                }
                io::Completion::socket_failure(token, -result)
            };
            return match buffer {
                None => Disposition::Public(completion),
                Some(buffer) => Disposition::Consumed(Some(buffer)),
            };
        }
        if op_kind == kind::OPEN {
            let completion = match cqe.into_opened(token) {
                Ok(fd) => io::Completion::opened(token, open::Opened::new(fd)),
                Err(errno) => io::Completion::open_failed(token, open::Error::from_errno(errno)),
            };
            return match buffer {
                None => Disposition::Public(completion),
                Some(buffer) => {
                    drop(completion);
                    Disposition::Consumed(Some(buffer))
                }
            };
        }
        if op_kind == kind::ACCEPT {
            return match buffer {
                Some(buffer) => Disposition::Consumed(Some(buffer)),
                None if result >= 0 => {
                    let slot = route::SlotIndex::from_bounded(result as u32);
                    let accepted =
                        handles::Accepted::from_live(handles::FixedSlot::from_index(slot));
                    Disposition::Public(io::Completion::accepted(token, accepted, more))
                }
                None => Disposition::Public(io::Completion::accept_failed(token, -result, more)),
            };
        }
        Self::public(token, result, more, buffer)
    }

    fn release_slots(&mut self, retired: driver::RetiredSlots<'d>) {
        let slots = self.drain.driver().outbound().take_retired_slots(retired);
        self.fixed_slots.release(slots);
    }

    fn public(
        token: route::Token,
        result: i32,
        more: bool,
        buffer: Option<uring::ffi::Buffer>,
    ) -> Disposition {
        if token == route::SHUTDOWN {
            return match buffer {
                None => Disposition::Public(io::Completion::shutdown()),
                buffer => Disposition::Consumed(buffer),
            };
        }
        let completion = match (token.kind(), buffer) {
            (kind::RECV, Some(buffer)) if result > 0 => {
                io::Completion::received(token, result as u32, more, buffer)
            }
            (kind::RECV, None) if result == -libc::ENOBUFS => {
                io::Completion::recv_exhausted(token, more)
            }
            (kind::RECV, None) if result <= 0 => io::Completion::operation(token, result, more),
            (
                kind::SEND
                | kind::READ
                | kind::WRITE
                | kind::STAT
                | kind::SYNC
                | kind::TUNING
                | kind::CONNECT,
                None,
            ) => io::Completion::operation(token, result, false),
            (_, buffer) => return Disposition::Consumed(buffer),
        };
        Disposition::Public(completion)
    }
}

//! Receive completion classification.

use std::mem;

use crate::{
    driver::route,
    io::{self, event, recv},
};

struct Payload<T> {
    more: bool,
    value: T,
}

#[repr(transparent)]
pub struct Completion<'d>(event::Targeted<Payload<io::RecvEvent<'d>>>);

/// A zero-copy receive completion refined to data.
#[repr(transparent)]
pub struct DataCompletion<'d>(event::Targeted<Payload<recv::Lease<'d>>>);

/// A receive completion refined to the non-data cases.
#[repr(transparent)]
pub struct ControlCompletion(event::Targeted<Payload<Control>>);

#[derive(Clone, Copy)]
pub enum Control {
    Eof,
    Cancelled,
    BufferExhausted,
    Starved,
    Failed(i32),
}

pub enum Classification<'d> {
    Data(DataCompletion<'d>),
    Control(ControlCompletion),
}

const _: () = {
    assert!(
        mem::size_of::<Completion<'static>>()
            == mem::size_of::<(route::Token, bool, io::RecvEvent<'static>)>()
    );
    assert!(
        mem::align_of::<Completion<'static>>()
            == mem::align_of::<(route::Token, bool, io::RecvEvent<'static>)>()
    );
    assert!(
        mem::size_of::<DataCompletion<'static>>()
            == mem::size_of::<(route::Token, bool, recv::Lease<'static>)>()
    );
    assert!(
        mem::align_of::<DataCompletion<'static>>()
            == mem::align_of::<(route::Token, bool, recv::Lease<'static>)>()
    );
    assert!(mem::size_of::<ControlCompletion>() == mem::size_of::<(route::Token, bool, Control)>());
    assert!(
        mem::align_of::<ControlCompletion>() == mem::align_of::<(route::Token, bool, Control)>()
    );
    assert!(mem::size_of::<Classification<'static>>() <= mem::size_of::<Completion<'static>>());
};

impl<T> Payload<T> {
    const fn new(more: bool, value: T) -> Self {
        Self { more, value }
    }

    const fn more(&self) -> bool {
        self.more
    }

    const fn value(&self) -> &T {
        &self.value
    }

    fn value_mut(&mut self) -> &mut T {
        &mut self.value
    }

    fn into_parts(self) -> (bool, T) {
        (self.more, self.value)
    }

    fn map<U>(self, map: impl FnOnce(T) -> U) -> Payload<U> {
        Payload::new(self.more, map(self.value))
    }
}

impl<'d> Completion<'d> {
    pub(in crate::io) const fn new(
        token: route::Token,
        more: bool,
        event: io::RecvEvent<'d>,
    ) -> Self {
        Self(event::Targeted::new(token, Payload::new(more, event)))
    }

    pub const fn token(&self) -> route::Token {
        self.0.token()
    }

    pub const fn more(&self) -> bool {
        self.0.value().more()
    }

    pub const fn event(&self) -> &io::RecvEvent<'d> {
        self.0.value().value()
    }

    pub fn event_mut(&mut self) -> &mut io::RecvEvent<'d> {
        self.0.value_mut().value_mut()
    }

    pub fn into_parts(self) -> (route::Token, bool, io::RecvEvent<'d>) {
        let (token, payload) = self.0.into_parts();
        let (more, event) = payload.into_parts();
        (token, more, event)
    }

    pub fn classify(self) -> Classification<'d> {
        let (token, payload) = self.0.into_parts();
        let (more, event) = payload.into_parts();
        match event {
            io::RecvEvent::Data(buffer) => Classification::Data(DataCompletion(
                event::Targeted::new(token, Payload::new(more, buffer)),
            )),
            io::RecvEvent::Eof => Classification::Control(ControlCompletion(event::Targeted::new(
                token,
                Payload::new(more, Control::Eof),
            ))),
            io::RecvEvent::Cancelled => Classification::Control(ControlCompletion(
                event::Targeted::new(token, Payload::new(more, Control::Cancelled)),
            )),
            io::RecvEvent::BufferExhausted => Classification::Control(ControlCompletion(
                event::Targeted::new(token, Payload::new(more, Control::BufferExhausted)),
            )),
            io::RecvEvent::Starved => Classification::Control(ControlCompletion(
                event::Targeted::new(token, Payload::new(more, Control::Starved)),
            )),
            io::RecvEvent::Failed(errno) => Classification::Control(ControlCompletion(
                event::Targeted::new(token, Payload::new(more, Control::Failed(errno))),
            )),
        }
    }
}

impl<'d> DataCompletion<'d> {
    pub const fn token(&self) -> route::Token {
        self.0.token()
    }

    pub const fn more(&self) -> bool {
        self.0.value().more()
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        self.0.value_mut().value_mut().as_mut_slice()
    }

    pub fn into_buffer(self) -> recv::Lease<'d> {
        let (_, payload) = self.0.into_parts();
        payload.into_parts().1
    }

    pub fn into_completion(self) -> Completion<'d> {
        Completion(self.0.map(|payload| payload.map(io::RecvEvent::Data)))
    }
}

impl ControlCompletion {
    pub const fn token(&self) -> route::Token {
        self.0.token()
    }

    pub const fn more(&self) -> bool {
        self.0.value().more()
    }

    pub const fn event(&self) -> Control {
        *self.0.value().value()
    }

    pub fn into_completion<'d>(self) -> Completion<'d> {
        Completion(self.0.map(|payload| {
            payload.map(|event| match event {
                Control::Eof => io::RecvEvent::Eof,
                Control::Cancelled => io::RecvEvent::Cancelled,
                Control::BufferExhausted => io::RecvEvent::BufferExhausted,
                Control::Starved => io::RecvEvent::Starved,
                Control::Failed(errno) => io::RecvEvent::Failed(errno),
            })
        }))
    }
}

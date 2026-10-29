use core::{marker, mem};

use crate::driver::route::{FrameworkTag, KeyTag, Select};

mod sealed;
pub(super) use sealed::Sealed;

/// Compile-time proof of one operation class accepted by the driver.
pub trait Class: Sealed + Copy {
    /// Operation class this marker selects.
    const KIND: Kind;

    /// Protocol code of this class, derived from its single class tag.
    const CODE: Code = Code::new(Self::KIND as u8);
}

/// An operation class which can own one native flight.
///
/// An unclassified target cannot enter the flight arena:
///
/// Framework controls do not own native flights:
pub trait Flight: Class {
    /// Native flight class of this operation class.
    const FLIGHT: Native;
}

/// The closed set of operation classes stored in the native flight arena.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Native {
    Accept = 1,
    Recv = 2,
    Send = 3,
    Socket = 5,
    Connect = 6,
    Open = 11,
    Tuning = 25,
    Write = 28,
    Sync = 29,
}

/// Opaque operation-class code used only by the route encoder.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Code(u8);

impl Code {
    pub(crate) const fn new(raw: u8) -> Self {
        Self(raw)
    }

    pub(crate) const fn raw(self) -> u8 {
        self.0
    }
}

const _: () = assert!(mem::size_of::<Code>() == mem::size_of::<u8>());

/// Zero-sized proof produced only by decoding one observed operation class.
#[derive(Clone, Copy)]
pub struct Witness<C: Class>(pub(super) marker::PhantomData<C>);

impl<C: Class> Witness<C> {
    const NEW: Self = Self(marker::PhantomData);

    pub(crate) const fn marker(self) -> marker::PhantomData<C> {
        self.0
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub(crate) enum Shape {
    Socket(Witness<Socket>),
    Open(Witness<Open>),
    Accept(Witness<Accept>),
    Recv(Witness<Recv>),
    Send(Witness<Send>),
    Write(Witness<Write>),
    Connect(Witness<Connect>),
    Sync(Witness<Sync>),
    Tuning(Witness<Tuning>),
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(u8)]
pub enum Kind {
    Unspecified = 0,
    Accept = 1,
    Recv = 2,
    Send = 3,
    Socket = 5,
    Connect = 6,
    Shutdown = 7,
    Open = 11,
    OneShot = 19,
    TaskQueue = 21,
    Tuning = 25,
    Write = 28,
    Sync = 29,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Unknown(u8);

#[derive(Clone, Copy, Debug)]
pub enum Any {}
#[derive(Clone, Copy)]
pub enum Accept {}
#[derive(Clone, Copy)]
pub enum Recv {}
#[derive(Clone, Copy)]
pub enum Send {}
#[derive(Clone, Copy)]
pub enum Socket {}
#[derive(Clone, Copy)]
pub enum Connect {}
#[derive(Clone, Copy)]
pub enum Shutdown {}
#[derive(Clone, Copy)]
pub enum Open {}
#[derive(Clone, Copy)]
pub enum OneShot {}
#[derive(Clone, Copy)]
pub enum TaskQueue {}
#[derive(Clone, Copy)]
pub enum Tuning {}
#[derive(Clone, Copy)]
pub enum Write {}
#[derive(Clone, Copy)]
pub enum Sync {}
impl Sealed for Any {}
impl Sealed for Accept {}
impl Sealed for Recv {}
impl Sealed for Send {}
impl Sealed for Socket {}
impl Sealed for Connect {}
impl Sealed for Shutdown {}
impl Sealed for Open {}
impl Sealed for OneShot {}
impl Sealed for TaskQueue {}
impl Sealed for Tuning {}
impl Sealed for Write {}
impl Sealed for Sync {}
impl Class for Any {
    const KIND: Kind = Kind::Unspecified;
}

impl Class for Accept {
    const KIND: Kind = Kind::Accept;
}

impl Class for Recv {
    const KIND: Kind = Kind::Recv;
}

impl Class for Send {
    const KIND: Kind = Kind::Send;
}

impl Class for Socket {
    const KIND: Kind = Kind::Socket;
}

impl Class for Connect {
    const KIND: Kind = Kind::Connect;
}

impl Class for Shutdown {
    const KIND: Kind = Kind::Shutdown;
}

impl Class for Open {
    const KIND: Kind = Kind::Open;
}

impl Class for OneShot {
    const KIND: Kind = Kind::OneShot;
}

impl Class for TaskQueue {
    const KIND: Kind = Kind::TaskQueue;
}

impl Class for Tuning {
    const KIND: Kind = Kind::Tuning;
}

impl Class for Write {
    const KIND: Kind = Kind::Write;
}

impl Class for Sync {
    const KIND: Kind = Kind::Sync;
}

impl<const ROUTE: u8> Select<Accept> for KeyTag<ROUTE, Accept> {}
impl<const ROUTE: u8> Select<Recv> for KeyTag<ROUTE, Recv> {}
impl<const ROUTE: u8> Select<Send> for KeyTag<ROUTE, Send> {}
impl<const ROUTE: u8> Select<Socket> for KeyTag<ROUTE, Socket> {}
impl<const ROUTE: u8> Select<Connect> for KeyTag<ROUTE, Connect> {}
impl<const ROUTE: u8> Select<Shutdown> for KeyTag<ROUTE, Shutdown> {}
impl<const ROUTE: u8> Select<Open> for KeyTag<ROUTE, Open> {}
impl<const ROUTE: u8> Select<OneShot> for KeyTag<ROUTE, OneShot> {}
impl<const ROUTE: u8> Select<TaskQueue> for KeyTag<ROUTE, TaskQueue> {}
impl<const ROUTE: u8> Select<Tuning> for KeyTag<ROUTE, Tuning> {}
impl<const ROUTE: u8> Select<Write> for KeyTag<ROUTE, Write> {}
impl<const ROUTE: u8> Select<Sync> for KeyTag<ROUTE, Sync> {}
impl Select<Accept> for FrameworkTag<Accept> {}
impl Select<Recv> for FrameworkTag<Recv> {}
impl Select<Send> for FrameworkTag<Send> {}
impl Select<Socket> for FrameworkTag<Socket> {}
impl Select<Connect> for FrameworkTag<Connect> {}
impl Select<Shutdown> for FrameworkTag<Shutdown> {}
impl Select<Open> for FrameworkTag<Open> {}
impl Select<OneShot> for FrameworkTag<OneShot> {}
impl Select<TaskQueue> for FrameworkTag<TaskQueue> {}
impl Select<Tuning> for FrameworkTag<Tuning> {}
impl Select<Write> for FrameworkTag<Write> {}
impl Select<Sync> for FrameworkTag<Sync> {}

impl Flight for Accept {
    const FLIGHT: Native = Native::Accept;
}

impl Flight for Recv {
    const FLIGHT: Native = Native::Recv;
}

impl Flight for Send {
    const FLIGHT: Native = Native::Send;
}

impl Flight for Socket {
    const FLIGHT: Native = Native::Socket;
}

impl Flight for Connect {
    const FLIGHT: Native = Native::Connect;
}

impl Flight for Open {
    const FLIGHT: Native = Native::Open;
}

impl Flight for Tuning {
    const FLIGHT: Native = Native::Tuning;
}

impl Flight for Write {
    const FLIGHT: Native = Native::Write;
}

impl Flight for Sync {
    const FLIGHT: Native = Native::Sync;
}

impl Native {
    pub(crate) fn witness<C: Flight>(self) -> Option<Witness<C>> {
        (self == C::FLIGHT).then_some(Witness::NEW)
    }
}

impl From<Native> for Shape {
    fn from(native: Native) -> Self {
        match native {
            Native::Socket => Self::Socket(Witness::NEW),
            Native::Open => Self::Open(Witness::NEW),
            Native::Accept => Self::Accept(Witness::NEW),
            Native::Recv => Self::Recv(Witness::NEW),
            Native::Send => Self::Send(Witness::NEW),
            Native::Write => Self::Write(Witness::NEW),
            Native::Connect => Self::Connect(Witness::NEW),
            Native::Sync => Self::Sync(Witness::NEW),
            Native::Tuning => Self::Tuning(Witness::NEW),
        }
    }
}

impl Unknown {
    pub(crate) const fn new(raw: u8) -> Self {
        Self(raw)
    }
}

const _: () = {
    assert!(mem::size_of::<Shape>() == 1);
    assert!(mem::size_of::<Native>() == mem::size_of::<Kind>());
};

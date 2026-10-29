use crate::memory;
pub mod create;
pub mod enter;
pub mod inspect;
pub mod layout;
pub mod map;
pub mod prepare;
pub mod ready;
pub mod register;
use self::{create::Create, enter::Enter, inspect::Inspect, map::Unmap, prepare::Prepare};
use crate::{
    io::{
        Close,
        reactor::{
            complete,
            complete::{Drain, Entry, message, packet::record},
            submit,
        },
    },
    kernel,
    memory::buffer::provided,
    probe::session::Closed,
};

/// Native ring ownership specialized to the caller's completion token.
pub trait Source: kernel::Source + Sized {
    type Decoded<T: Copy, E>: record::Map<
            Parts = complete::Parts<
                T,
                <Self::Entry<T> as Entry>::Outcome,
                <Self::Entry<T> as Entry>::Flags,
            >,
            Fault = E,
        >;
    type Ready<T: complete::raw::Token, L: memory::raw::Receive>: ready::Ready<
            Ring = Self::Ring<T>,
            Token = T,
            Layout = L,
            Context = <Self as provided::Source>::Context<L>,
            Producer = <Self as provided::Source>::Producer<L>,
            Pending = <Self as provided::Source>::Pending<L::Extent>,
            Index = <Self as provided::Source>::Index,
            Entry = <Self as Source>::Entry<T>,
        > + ready::raw::Create
    where
        Self: provided::Source;
    type Poll: Copy + Eq;
    type Entry<T>: complete::Entry<Token = T, Outcome: complete::Outcome<Returned = Self::Returned>>;
    type Packet<T: Copy, B>: Packet<
            Native = Self,
            Parts = complete::Parts<
                T,
                <Self::Entry<T> as complete::Entry>::Outcome,
                <Self::Entry<T> as complete::Entry>::Flags,
            >,
            Buffer = B,
        >;
    type Message<'a, const NAME: usize, const CONTROL: usize>: message::Output<'a>;
    type Enter<'a, T: 'a>: Enter;
    type Ring<T: complete::raw::Token>: Create
        + Owner<Token = T>
        + Prepare
        + Inspect<Code = Self::Code>
        + Complete<Entry = Self::Entry<T>>;
}

/// Retains the native provider of an owned ring completion without runtime storage.
pub trait Packet: complete::Packet {
    type Native: Source;
}

/// Exclusive queue and registration views borrow the same native owner.
pub trait Access {
    type Token;
    type Target;
    type File: register::Occupied<Target = Self::Target>;
    type Queue<'a>: submit::Queue<'a, Self::Token>
    where
        Self: 'a,
        Self::Token: complete::raw::Token;
    type Enter<'a>: Enter
    where
        Self: 'a;
    type Registry<'a>: register::Registry<Token = Self::Token, Target = Self::Target, File = Self::File>
    where
        Self: 'a;

    fn sq(&mut self) -> Self::Queue<'_>
    where
        Self::Token: complete::raw::Token;
    fn enter(&mut self) -> Self::Enter<'_>;
    fn registry(&mut self) -> Self::Registry<'_>;
}

/// Ring registration and failed fixed-file cleanup retain the complete owner.
pub trait Owner: Access + Close<State: Unmap> + Sized {
    type Registered: register::Registered<
            Token = Self::Token,
            State: map::Unmap<Error = <Self::State as map::Unmap>::Error>,
        >;
    type Registration: register::Registration;
    type Closing: Closed<Target = Self::Target>;

    fn try_register(self) -> Result<Self::Registered, Self::Registration>;
    fn close_file(self, file: Self::File) -> Result<(), Self::Closing>;
}

pub trait Complete {
    type Entry: Entry;
    type Queue<'a>: Drain<Entry = Self::Entry>
    where
        Self: 'a;

    fn completion(&mut self) -> Self::Queue<'_>;
}

use dope_sys::{
    engine::admit,
    io::{
        self, file,
        reactor::{
            complete::Vacancy,
            read::Arm as _,
            ready::{
                self, store,
                store::{Exec, State as _, submit},
            },
            retry,
        },
        request::{self, Execute as _},
        socket::addr,
    },
    table::change,
};

use o3::buffer::bytes;

use crate::{Kqueue, reactor::Guard};
type Key<C> = <<C as ready::Driver>::Context as Exec>::Key;
type Slot<C> =
    <<<C as ready::Driver>::Context as store::Context>::Slots as change::raw::Slots>::Physical;

impl<'a, C, V> admit::raw::Retain<C> for Guard<'a, C::State, V>
where
    C: submit::Context + admit::Context<Native = Kqueue, Key = Key<C>>,
    C::Context: 'a,
    V: Vacancy<Event = <C::Context as Exec>::Event>,
    C::Inputs: request::Context<Target = Slot<C>>,
{
    unsafe fn send(
        self,
        slot: Slot<C>,
        input: bytes::Prefix<'_, { i32::MAX as usize }>,
        key: Key<C>,
    ) {
        let write = <<C::Context as Exec>::Send as retry::raw::Send<
            <C::Context as Exec>::Writer,
        >>::bytes(input);
        // SAFETY: the host retains the bytes through completion or quiescence.
        unsafe { retry::raw::Write::send(&mut self.state.write(), key, slot, write, self.vacant) };
    }

    unsafe fn send_msg<'r>(self, slot: Slot<C>, message: impl io::raw::Message<'r>, key: Key<C>) {
        let write = <<C::Context as Exec>::Send as retry::raw::Send<
            <C::Context as Exec>::Writer,
        >>::message(message.header());
        // SAFETY: the host retains the message and its complete input graph.
        unsafe { retry::raw::Write::send(&mut self.state.write(), key, slot, write, self.vacant) };
    }

    unsafe fn accept(self, listener: Slot<C>, peer: &mut addr::Storage, key: Key<C>) {
        let (address, length) = peer.prepare_output();
        self.state
            .read()
            .accept_one(key, listener, address, length, self.vacant);
    }

    unsafe fn connect(self, slot: Slot<C>, address: addr::Addr<'_>, key: Key<C>) {
        retry::raw::Write::connect(&mut self.state.write(), key, slot, address, self.vacant);
    }

    unsafe fn file(self, request: <Kqueue as file::File>::Request<'_>, key: Key<C>) {
        self.vacant.push(request.execute(C::file(key)));
    }
}

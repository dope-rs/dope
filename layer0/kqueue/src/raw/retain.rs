use dope_sys::{
    engine::admit,
    io::{
        file,
        reactor::{
            complete::{self, Vacancy},
            read::Arm as _,
            ready::{
                self, store,
                store::{Exec, State as _, submit},
            },
            retry,
        },
        request::{self, Execute as _},
    },
    table::change,
};

use crate::{Command, Kqueue, command::Retained, reactor::Guard};
type Key<C> = <<C as ready::Driver>::Context as Exec>::Key;
type Slot<C> =
    <<<C as ready::Driver>::Context as store::Context>::Slots as change::raw::Slots>::Physical;

impl<C> submit::raw::Retain<C> for Command<Retained<'_, Slot<C>>>
where
    C: submit::Context,
{
    unsafe fn retain<'a>(
        self,
        state: &'a mut C::State,
        key: <C::Context as Exec>::Key,
        vacant: impl complete::Vacancy<Event = <C::Context as Exec>::Event>,
    ) where
        C::Context: 'a,
    {
        match self.0 {
            Retained::Accept {
                listener,
                addr,
                len,
            } => state.read().accept_one(key, listener, addr, len, vacant),
            Retained::Send { slot, input } => unsafe {
                retry::raw::Write::send(
                    &mut state.write(),
                    key,
                    slot,
                    || {
                        <<C::Context as Exec>::Send as retry::raw::Send<
                            <C::Context as Exec>::Writer,
                        >>::bytes(input)
                    },
                    vacant,
                )
            },
            Retained::SendMsg { slot, msg } => unsafe {
                retry::raw::Write::send(
                    &mut state.write(),
                    key,
                    slot,
                    || {
                        <<C::Context as Exec>::Send as retry::raw::Send<
                            <C::Context as Exec>::Writer,
                        >>::message(msg)
                    },
                    vacant,
                )
            },
            Retained::Connect { slot, addr } => {
                retry::raw::Write::connect(&mut state.write(), key, slot, addr, vacant)
            }
        }
    }
}

impl<'a, C, V> admit::raw::Retain<C> for Guard<'a, C::State, V>
where
    C: submit::Context + admit::Context<Native = Kqueue, Key = Key<C>>,
    C::Context: 'a,
    V: Vacancy<Event = <C::Context as Exec>::Event>,
    C::Inputs: request::Context<Target = Slot<C>>,
{
    unsafe fn retain(self, request: Command<Retained<'_, Slot<C>>>, key: Key<C>) {
        // SAFETY: the host retains every request pointee until completion or quiescence.
        unsafe { submit::raw::Retain::<C>::retain(request, self.state, key, self.vacant) };
    }

    unsafe fn file(self, request: <Kqueue as file::File>::Request<'_>, key: Key<C>) {
        self.vacant.push(request.execute(C::file(key)));
    }
}

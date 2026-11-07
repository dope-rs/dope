use dope_sys::{
    io::{
        reactor::{
            complete,
            read::Arm as _,
            ready::{
                Driver,
                delivery::entry::Create as _,
                store::{self, Exec, State as _, submit},
            },
            retry,
        },
        request::Lower,
        socket::{Domain, Kind, Prepare as _},
    },
    table::change,
};
pub(crate) use lowered::Lowered;
pub(crate) use message::Message;
use o3::base::marker::ThreadBound;

use crate::{Kqueue, sockets::Handle};

mod lowered;
mod message;

type Slot<C> = <<<C as Driver>::Context as store::Context>::Slots as change::raw::Slots>::Physical;

/// One backend request retaining its selected representation.
#[repr(transparent)]
pub struct Command<T>(pub(crate) T, ThreadBound);

impl<T> Command<T> {
    fn new(inner: T) -> Self {
        const {
            assert!(std::mem::size_of::<Self>() == std::mem::size_of::<T>());
            assert!(std::mem::align_of::<Self>() == std::mem::align_of::<T>());
        }
        Self(inner, ThreadBound::NEW)
    }
}

impl<I, O, D, M> Lower for Command<Lowered<I, O, D, M>> {
    type Target = I;
    type Outbound = O;
    type Domain = D;
    type Datagram = M;

    fn socket(slot: Self::Outbound, socket: Self::Domain) -> Self {
        Self::new(Lowered::SocketAt { socket, slot })
    }

    fn accept_multi(listener: Self::Target) -> Self {
        Self::new(Lowered::AcceptMultishot { listener })
    }

    fn recv<const GROUP: u16>(slot: Self::Target) -> Self {
        Self::new(Lowered::RecvMulti { slot })
    }

    fn recv_msg<const GROUP: u16>(message: Self::Datagram) -> Self {
        Self::new(Lowered::RecvMsgMulti(message))
    }

    fn shutdown(slot: Self::Target) -> Self {
        Self::new(Lowered::ShutdownWrite { slot })
    }
}

impl<C> submit::Execute<C> for Command<Lowered<Slot<C>, C::Outbound, Domain, Message<Slot<C>>>>
where
    C: submit::Context<Handle = Handle>,
{
    fn execute<'a>(
        self,
        state: &'a mut C::State,
        key: <C::Context as Exec>::Key,
        vacant: impl complete::Vacancy<Event = <C::Context as Exec>::Event>,
    ) where
        C::Context: 'a,
    {
        match self.0 {
            Lowered::AcceptMultishot { listener } => {
                state.read().accept_many(key, listener, vacant)
            }
            Lowered::RecvMulti { slot } => state.read().recv(key, slot, vacant),
            Lowered::RecvMsgMulti(message) => state.read().message(key, message, vacant),
            Lowered::ShutdownWrite { slot } => {
                retry::raw::Write::shutdown(&mut state.write(), key, slot, vacant)
            }
            Lowered::SocketAt { socket, slot } => vacant.push(C::Created::created(
                key,
                slot,
                Kqueue::open(socket, Kind::Stream),
            )),
        }
    }
}

use std::os::fd;

use crate::{
    Errno,
    io::reactor::{
        complete::{self, resolve},
        read,
        ready::delivery::entry::{Context, Create, Effects, Key, Resolved, Retired, Socket},
        retry,
    },
};

type Reclamation<'q, 'd, C, B> =
    resolve::Reclaimed<B, Retired<C>, <C as Effects<'q, 'd, B>>::Error>;

pub trait Event<C: Context, B>:
    Create<C::Outbound, C::Handle, Key = <C::Reader as read::Context>::Key, Output = Self>
    + complete::Event<Key = Key<C>>
    + complete::Map
    + Sized
{
    fn opened(key: Key<C>, result: Result<fd::OwnedFd, Errno>) -> Self;
    fn written(key: Key<C>, result: <C::Writer as retry::Context>::Outcome) -> Self;
    fn synced(key: Key<C>, result: <C::Writer as retry::Context>::Unit) -> Self;
    const SHUTDOWN: Self;
    fn resolve<'q, 'd>(
        self,
        files: &mut C::Files,
        drain: &C::Drain,
    ) -> Resolved<C::Output, Self, Reclamation<'q, 'd, C, B>>
    where
        C: Socket<'q, 'd, B>;
    fn reclaim<'q, 'd>(self, drain: &C::Drain) -> Reclamation<'q, 'd, C, B>
    where
        C: Socket<'q, 'd, B>;
}

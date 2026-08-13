use std::marker;

pub trait AmbiguousIfSend<A> {}

impl<T: ?Sized> AmbiguousIfSend<()> for T {}

impl<T: ?Sized + Send> AmbiguousIfSend<u8> for T {}

pub trait AmbiguousIfSync<A> {}

impl<T: ?Sized> AmbiguousIfSync<()> for T {}

impl<T: ?Sized + Sync> AmbiguousIfSync<u8> for T {}

pub trait AmbiguousIfUnpin<A> {}

impl<T: ?Sized> AmbiguousIfUnpin<()> for T {}

impl<T: ?Sized + Unpin> AmbiguousIfUnpin<u8> for T {}

pub struct Affinity<T: ?Sized>(marker::PhantomData<fn() -> T>);

impl<T: ?Sized> Affinity<T> {
    pub fn not_send<A>()
    where
        T: AmbiguousIfSend<A>,
    {
    }

    pub fn not_sync<A>()
    where
        T: AmbiguousIfSync<A>,
    {
    }

    pub fn not_unpin<A>()
    where
        T: AmbiguousIfUnpin<A>,
    {
    }

    pub fn require_send()
    where
        T: Send,
    {
    }
}

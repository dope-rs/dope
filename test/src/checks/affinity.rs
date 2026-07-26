pub trait AmbiguousIfSend<A> {}

impl<T: ?Sized> AmbiguousIfSend<()> for T {}

impl<T: ?Sized + Send> AmbiguousIfSend<u8> for T {}

pub trait AmbiguousIfSync<A> {}

impl<T: ?Sized> AmbiguousIfSync<()> for T {}

impl<T: ?Sized + Sync> AmbiguousIfSync<u8> for T {}

pub trait AmbiguousIfUnpin<A> {}

impl<T: ?Sized> AmbiguousIfUnpin<()> for T {}

impl<T: ?Sized + Unpin> AmbiguousIfUnpin<u8> for T {}

pub fn not_send<T: ?Sized + AmbiguousIfSend<A>, A>() {}

pub fn not_sync<T: ?Sized + AmbiguousIfSync<A>, A>() {}

pub fn not_unpin<T: ?Sized + AmbiguousIfUnpin<A>, A>() {}

pub fn require_send<T: Send>() {}

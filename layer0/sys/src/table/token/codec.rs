use crate::{
    io::reactor::complete,
    table::token::{Close, Context, Decoded, Tuning, frame, host},
};

type Binding<C, D> = host::Bound<
    <D as host::Drain>::Echo,
    <D as host::Drain>::Class,
    <C as Codec>::Frame,
    <D as host::Drain>::Flight,
    <<C as Codec>::Context as host::Context>::Key,
    <C as Codec>::Tuning,
>;
type Frame<C> = Decoded<
    <<C as Codec>::Context as host::Context>::Key,
    <<C as Codec>::Context as host::Context>::Returned,
    <C as Codec>::Frame,
    <C as Codec>::Tuning,
>;

/// Native identity construction and classification, before host ownership binding.
pub trait Codec: complete::raw::Token {
    type Context: host::Context;
    type Close<'d>: Close<'d, Context = Self::Context>;
    type Tuning: Tuning<Slot = <Self::Context as Context>::Slot>;
    type Frame: frame::Decode<Context = Self::Context, Tuning = Self::Tuning>;

    fn close(control: Self::Close<'_>) -> Self;
    fn flight(key: <Self::Context as host::Context>::Key) -> Self;
    fn prepare() -> Self;
    fn probe(probe: <Self::Context as host::Context>::Probe) -> Self;
    fn cancel_probe(probe: <Self::Context as host::Context>::Probe) -> Self;
    fn shutdown() -> Self;
    fn tuning(control: Self::Tuning) -> Self;
    fn cancel_key(key: <Self::Context as host::Context>::Key) -> Self;
    fn cancel_tune(control: Self::Tuning) -> Self;
    fn cancel(cancel: host::Cancel<<Self::Context as host::Context>::Key, Self::Tuning>) -> Self;
    fn bind<D>(self, drain: &D) -> Binding<Self, D>
    where
        D: host::Drain<
                Key = <Self::Context as host::Context>::Key,
                Returned = <Self::Context as host::Context>::Returned,
            >;
    fn closed(self) -> Option<<Self::Context as host::Context>::Returned>;
    fn decode(self) -> Frame<Self>;
}

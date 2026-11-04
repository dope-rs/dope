pub(super) use binding::Binding;
use dope_sys::{
    io::{
        reactor::{complete, complete::Observe},
        socket::option::tuning,
    },
    kernel,
    memory::buffer::provided,
    ring,
    table::token,
};
pub(crate) use envelope::{Envelope, Rejected};
pub(crate) use tuned::Tuned;

use crate::{
    Backend,
    driver::{
        life::native::host::{Selection, context::Context},
        receive::tokens,
        retained::flight,
    },
    io::{socket::option, transfer},
};

mod binding;
mod envelope;
mod tuned;

type Token = <Backend as token::Source>::Token<Context>;
type Frame = <Token as token::Codec>::Frame;
type Tune = <Token as token::Codec>::Tuning;
type CloseToken<'d> = <Token as token::Codec>::Close<'d>;
type Selected = <Backend as provided::Source>::Pending<transfer::Extent>;
type Returned = <Backend as kernel::Source>::Returned;
type Entry = <Backend as ring::Source>::Entry<Token>;
type Scalar = <Entry as complete::Entry>::Outcome;
type Value = <Scalar as complete::Outcome>::Value;
type Observation = <Backend as Observe<Token>>::Observation;
type Record = <Backend as ring::Source>::Decoded<Token, Selection>;
type Decoded<'d> = <Record as complete::Map>::Entry<tokens::Token<'d, Selected>>;
type Packet<'d> = <Backend as ring::Source>::Packet<Token, tokens::Token<'d, Selected>>;
type Tuning = <Backend as tuning::Source>::Table<flight::raw::Echo, { option::MAX_STREAM_OPTIONS }>;
type Occupied<'state> =
    <Tuning as tuning::Table<flight::raw::Echo, { option::MAX_STREAM_OPTIONS }>>::Occupied<'state>;

pub(super) use binding::Binding;
use dope_sys::{
    io::{
        file,
        reactor::{complete, complete::Observe},
        socket::option::tuning::Source,
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
        retained::flight,
    },
    io::{socket::option, transfer},
};

mod binding;
mod envelope;
mod tuned;

type Token<N> = <N as token::Source>::Token<Context>;
type Frame<N> = <Token<N> as token::Codec>::Frame;
type Tune<N> = <Token<N> as token::Codec>::Tuning;
type CloseToken<'d, N> = <Token<N> as token::Codec>::Close<'d>;
type Selected<N> = <N as provided::Source>::Pending<transfer::Extent>;
type Completed<N> = <N as provided::Source>::Complete<transfer::Extent>;
type Opened<N> = <N as file::Completion>::Opened;
type Returned<N = Backend> = <N as kernel::Source>::Returned;
type Entry<N> = <N as ring::Source>::Entry<Token<N>>;
type Scalar<N> = <Entry<N> as complete::Entry>::Outcome;
type Flags<N> = <Entry<N> as complete::Entry>::Flags;
type Value<N> = <Scalar<N> as complete::Outcome>::Value;
type Parts<N> = complete::Parts<Token<N>, Scalar<N>, Flags<N>>;
type Observation<N> = <N as Observe<Token<N>>>::Observation;
type Record<N> = <N as ring::Source>::Decoded<Token<N>, Selection>;
type Decoded<B, N> = <Record<N> as complete::Map>::Entry<B>;
type Tuning<N> = <N as Source>::Table<flight::raw::Echo, { option::MAX_STREAM_OPTIONS }>;

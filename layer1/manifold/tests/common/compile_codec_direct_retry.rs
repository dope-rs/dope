use dope_manifold::client;
use o3::buffer::{self, hold, resident};

use client::session::{codec, response};
use codec::staging;

fn retry<'d, C, S>(
    codec: &C,
    storage: &mut C::Storage<S>,
    state: &mut C::State<'d>,
    budget: &resident::Budget<'d>,
    limit: staging::Limit,
) where
    C: codec::Stateful,
    S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
{
    let _ = codec.decode::<S, response::Direct, _>(storage, state, budget, limit, |_| {
        codec::Admission::Ready(
            |_: Result<C::Outcome<'d>, C::Error>, _: &mut C::State<'d>| -> response::Direct {
                response::Direct::RetryInput
            },
        )
    });
}

fn main() {}

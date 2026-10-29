use std::hint;

use dope_manifold::client;
use o3::buffer::{self, hold, resident};

use client::session::{codec, response};
use codec::staging;

fn retain<'d, C, S>(
    codec: &C,
    storage: &mut C::Storage<S>,
    state: &mut C::State<'d>,
    budget: &resident::Budget<'d>,
    limit: staging::Limit,
) where
    C: for<'a> codec::Stateful<Preview<'a> = &'a [u8]>,
    S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
{
    let _ = codec.decode::<S, response::Direct, _>(storage, state, budget, limit, |preview| {
        let Ok(input) = preview else {
            return codec::Admission::RetryInput;
        };
        codec::Admission::Ready(
            move |_: Result<C::Outcome<'d>, C::Error>, _: &mut C::State<'d>| {
                hint::black_box(input);
            },
        )
    });
}

fn main() {}

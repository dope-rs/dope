use std::hint;

use dope_manifold::client;
use o3::buffer::{self, hold, resident};

use client::session::{codec, response};
use codec::{staging, storage::Slot};

fn retain<'d, C, S>(
    codec: &C,
    storage: C::Storage<S>,
    state: C::State<'d>,
    budget: &resident::Budget<'d>,
    limit: staging::Limit,
) where
    C: codec::Stateful,
    S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
{
    let _ = Slot::new((storage, state)).run(|input| {
        codec.finish::<S, response::Direct, _>(input, budget, limit, |preview| {
            let Err(error) = preview else {
                return codec::Admission::RetryInput;
            };
            codec::Admission::Ready(
                move |_: Result<C::Outcome<'d>, C::Error>, _: &mut C::State<'d>| {
                    hint::black_box(error);
                },
            )
        })
    });
}

fn main() {}

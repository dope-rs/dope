use std::hint;

use dope_manifold::client;
use o3::buffer::{self, hold, resident};

use client::session::{codec, response};
use codec::{staging, storage::Slot};

fn accept<'d, C, S>(
    codec: &C,
    mut storage: C::Storage<S>,
    mut state: C::State<'d>,
    budget: &resident::Budget<'d>,
    limit: staging::Limit,
) where
    C: for<'a> codec::Stateful<Preview<'a> = &'a [u8]>,
    S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
{
    let _ = codec.decode::<S, response::Direct, _>(
        &mut storage,
        &mut state,
        budget,
        limit,
        |preview| {
            let length = preview.map_or(0, <[u8]>::len);
            codec::Admission::Ready(
                move |_: Result<C::Outcome<'d>, C::Error>, _: &mut C::State<'d>| {
                    hint::black_box(length);
                },
            )
        },
    );
    let _ = Slot::new((storage, state)).run(|input| {
        codec.finish::<S, response::Delivery<usize>, _>(input, budget, limit, |preview| {
            let length = preview.map_or(0, <[u8]>::len);
            codec::Admission::Ready(
                move |_: Result<C::Outcome<'d>, C::Error>, _: &mut C::State<'d>| Some(length),
            )
        })
    });
}

fn main() {}

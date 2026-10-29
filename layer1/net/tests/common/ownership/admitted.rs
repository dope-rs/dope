use std::num::NonZero;

use dope_core::io::transfer::Len;
use dope_net::{
    link::{
        egress::{
            NonEmpty,
            data::pair,
            load::lease,
            metadata::{self, Lane, arena, batch::Rejected},
        },
        event::{Completion, Kind},
        pool::pending::Round,
    },
    wire::{
        Identity, Spec,
        receive::{Chunk, batch::Produced, mode, scoped::Input},
        send::{self, reclaim},
    },
};
use o3::{
    alloc,
    base::cell::region,
    buffer::{
        self, pool,
        storage::range,
        write::consume::{self, Consume as _},
    },
};

fn reopen_arena<'p, 'd>(
    token: &region::Token<region::Identity<'d>>,
    store: &'p mut arena::Store<'d>,
) -> Result<arena::Arena<'p, 'd, &'d [u8]>, alloc::Error> {
    let first = arena::Arena::<&[u8]>::try_new(token, store)?;
    drop(first);
    arena::Arena::<&[u8]>::try_new(token, store)
}

fn borrow_lease<'a, R: lease::Commit + 'a>(owner: R, body: NonEmpty<R::Item>) -> impl Sized + 'a
where
    R::Item: 'a,
{
    body.lease(owner).into_inner()
}

fn checked_geometry<C: buffer::raw::Segment>(
    prefix: pool::Cursor,
    body: C,
) -> Result<pair::Source<C>, pair::Rejected<C>> {
    let value = pair::Source::try_new(prefix, body)?;
    Ok(value.into_inner())
}

fn retain_produced<'id, B>(output: Produced<'id, B>) -> Produced<'id, B> {
    output
}

fn consume_after_plan<'input, 'w, 'd: 'w>(
    input: &'input mut Input<'input, 'w, 'd, Identity>,
) -> Result<(Result<(), consume::Error>, usize), range::Error> {
    let plan = input.retention().try_plan(0..1)?;
    let len = plan.as_slice().len();
    drop(plan);
    Ok((input.try_consume(1), len))
}

fn retain_chunk<'w, 'd: 'w>(chunk: Chunk<'w, 'd, Identity>) -> Chunk<'w, 'd, Identity> {
    chunk
}

fn retain_credit<'w, 'd: 'w, W: Spec + 'w>(
    chunk: Chunk<'w, 'd, W, mode::Enabled>,
) -> Chunk<'w, 'd, W, mode::Enabled> {
    chunk
}

fn classify_completion(done: Completion<'_>) {
    match done.classify() {
        Kind::Data(data) => drop(data),
        Kind::Control(ctrl) => drop(ctrl),
    }
}

fn reborrow_storage() -> Len {
    let mut storage = ();
    let prepared = send::Storage::new(&mut storage).empty::<reclaim::OnSubmit>();
    let count = prepared.into_parts().1;
    let alias = &mut storage;
    let () = *alias;
    count
}

fn retain_round<'p, 'd>(round: Round<'p, 'd>) -> Round<'p, 'd> {
    round
}

fn release_front<'d>(
    store: &mut arena::Store<'d>,
    token: &mut region::Token<region::Identity<'d>>,
) -> Result<Option<&'d [u8]>, alloc::Error> {
    let value = {
        let arena = arena::Arena::<&[u8]>::try_new(token, store)?;
        let Some(slot) = arena.get(Lane::at(NonZero::<u32>::MIN)) else {
            return Ok(None);
        };
        let Some(front) = slot.queue().take_front(token) else {
            return Ok(None);
        };
        *front.get()
    };
    drop(arena::Arena::<&[u8]>::try_new(token, store)?);
    Ok(Some(value))
}

fn retain_rejected<'a>(rejected: Rejected<&'a [u8]>) -> Rejected<&'a [u8]> {
    rejected
}

fn reenter_after_front<'p, 'd>(
    queue: metadata::Queue<'_, 'p, 'd, &'d [u8]>,
    token: &mut region::Token<region::Identity<'d>>,
) -> Option<&'d [u8]> {
    let front = queue.take_front(token)?;
    let value = *front.get();
    drop(front);
    drop(queue.take_front(token));
    Some(value)
}

fn main() {}

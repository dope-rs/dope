use dope_net::wire::{Identity, receive::scoped::Input};
use o3::buffer::{
    storage::range,
    write::consume::{self, Consume as _},
};

fn advance_behind_plan<'input, 'w, 'd: 'w>(
    input: &'input mut Input<'input, 'w, 'd, Identity>,
) -> Result<(Result<(), consume::Error>, usize), range::Error> {
    let plan = input.retention().try_plan(0..1)?;
    let consumed = input.try_consume(1);
    Ok((consumed, plan.as_slice().len()))
}

fn main() {}

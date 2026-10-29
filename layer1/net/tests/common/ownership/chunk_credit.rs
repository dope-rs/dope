use dope_net::wire::{
    Spec,
    receive::{Chunk, mode},
};

fn erase<'w, 'd: 'w, W: Spec + 'w>(
    chunk: Chunk<'w, 'd, W, mode::Enabled>,
) -> Chunk<'w, 'd, W, mode::Disabled> {
    chunk
}

fn main() {}

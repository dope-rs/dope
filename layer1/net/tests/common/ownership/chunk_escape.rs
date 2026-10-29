use dope_net::wire::{Identity, receive::Chunk};

fn widen<'w, 'd: 'w, 'out: 'w>(chunk: Chunk<'w, 'd, Identity>) -> Chunk<'w, 'out, Identity> {
    chunk
}

fn main() {}

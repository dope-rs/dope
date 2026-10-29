use dope_manifold::client::session::codec::storage::Owner;

fn substitute<'slot, 'first, 'second>(
    first: Owner<'slot, 'first, u8, ()>,
    second: Owner<'slot, 'second, u8, ()>,
) -> Owner<'slot, 'first, u8, ()> {
    drop(first);
    second
}

fn main() {}

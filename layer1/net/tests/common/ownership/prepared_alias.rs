use dope_core::io::transfer::Len;
use dope_net::wire::send::{self, reclaim};

fn borrowed_storage() -> Len {
    let mut storage = ();
    let prepared = send::Storage::new(&mut storage).empty::<reclaim::OnSubmit>();
    let alias = &mut storage;
    let count = prepared.into_parts().1;
    let () = *alias;
    count
}

fn main() {}

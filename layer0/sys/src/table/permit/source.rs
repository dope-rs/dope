use o3::depot;

use crate::table::permit;

/// Native selection of storage retaining the caller's key and index proofs.
pub trait Source {
    type Queue<K: permit::raw::Key, I: depot::raw::Compact>: permit::Queue<Key = K, Index = I>;
}

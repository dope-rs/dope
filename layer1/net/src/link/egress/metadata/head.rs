use o3::base::cell::region;

use crate::link::egress::{
    self,
    load::credit,
    metadata::{self, pool::store::index::Index},
    reserve::demand,
};

pub(in crate::link::egress::metadata) fn detach<
    'p,
    'd,
    T,
    M: credit::Access<'p, 'd>,
    D: demand::Mode,
>(
    queue: metadata::Queue<'_, 'p, 'd, T, M, D>,
    token: &mut region::Token<region::Identity<'d>>,
) -> Option<(egress::NonEmpty<T>, Index<'d>, credit::Stored<'p>)> {
    let metadata::Prefix { queue, prefix } = queue.prefix()?;
    let index = prefix.index();
    let (next, value, stored) = queue.pool.store.detach(token, index);
    let (slot, stored) = prefix.pop(|slot| (next, slot, stored));
    Some((value, slot, stored))
}

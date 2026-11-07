use crate::conn::stream::state::streams::closed;
use crate::conn::stream::state::streams::closed::storage::index::{Index, RawIndex};
use crate::conn::stream::state::streams::closed::storage::node::{Gap, Key};
use crate::conn::stream::state::streams::closed::tree::edit::Edit;
use crate::conn::stream::state::streams::closed::tree::entry::Location;
use crate::conn::stream::state::streams::closed::tree::path::Path;

pub(super) struct Remove<'edit, 'owner, L> {
    edit: &'edit mut Edit<'owner, L>,
    path: Path<'owner, L>,
    location: Location<'owner, L>,
}

pub(super) struct Removed;

impl<'edit, 'owner, L> Remove<'edit, 'owner, L> {
    pub(super) const fn new(
        edit: &'edit mut Edit<'owner, L>,
        path: Path<'owner, L>,
        location: Location<'owner, L>,
    ) -> Self {
        Self {
            edit,
            path,
            location,
        }
    }

    pub(super) fn commit(mut self) -> Option<Removed> {
        let (leaf, key) = self.leaf()?;
        let node = self.edit.node_mut(leaf)?;
        if !node.is_leaf() {
            return None;
        }
        node.remove::<true>(key)?;
        self.rebalance()?;

        let root = self.edit.root.brand();
        let node = self.edit.node(root)?;
        if node.len() != 0 {
            return Some(Removed);
        }
        *self.edit.root = if node.is_leaf() {
            RawIndex::NONE
        } else {
            node.child(Gap::at::<0>())
        };
        self.edit.release(root)?;
        Some(Removed)
    }

    fn leaf(&mut self) -> Option<(Index<'owner, L>, Key)> {
        let depth = self.location.depth;
        let target = self.location.node;
        let key = self.location.key;
        let node = self.edit.node(target)?;
        if node.is_leaf() {
            return Some((target, key));
        }

        let mut index = node.child(key.widen()).brand();
        self.path.trim(depth, key.widen());
        for _ in 0..closed::MAX_LEVELS {
            let node = self.edit.node(index)?;
            let prior = node.last()?;
            let interval = node.key(prior);
            let gap = node.end();
            let leaf = node.is_leaf();
            let child = (!leaf).then(|| node.child(gap));
            let vacant = self.path.vacant()?;
            vacant.insert(index, gap);
            if leaf {
                *self.edit.node_mut(target)?.key_mut(key)? = interval;
                return Some((index, prior));
            }
            let child = child?;
            index = child.brand();
        }
        None
    }

    fn rebalance(&mut self) -> Option<Removed> {
        let mut path = self.path.steps().rev();
        let mut child = path.next()?;
        for parent in path {
            let index = child.node();
            let node = self.edit.node(index)?;
            if node.len() >= closed::MIN_KEYS {
                return Some(Removed);
            }
            let leaf = node.is_leaf();
            let at = parent.gap();
            let owner = parent.node();
            let node = self.edit.node(owner)?;
            let left = node.left(at);
            let right = node.right(at);
            if let Some((left, key_at)) = left {
                let left = left.brand();
                let peer = self.edit.node(left)?;
                let peer_len = peer.len();
                if peer_len > closed::MIN_KEYS {
                    let pivot = node.key(key_at);
                    if leaf != peer.is_leaf() {
                        return None;
                    }
                    let last = peer.last()?;
                    let key = peer.key(last);
                    let branch = peer.child(peer.end());

                    let node = self.edit.node_mut(index)?;
                    node.insert::<false>(Gap::at::<0>(), pivot, (!leaf).then_some(branch))?;
                    *self.edit.node_mut(owner)?.key_mut(key_at)? = key;
                    self.edit.node_mut(left)?.set_len(peer_len - 1)?;
                    return Some(Removed);
                }
            }
            if let Some((right, key_at)) = right {
                let right = right.brand();
                let peer = self.edit.node(right)?;
                if peer.len() > closed::MIN_KEYS {
                    let pivot = node.key(key_at);
                    if leaf != peer.is_leaf() {
                        return None;
                    }
                    let first = Key::at::<0>();
                    let key = peer.key(first);
                    let branch = peer.child(Gap::at::<0>());

                    let node = self.edit.node_mut(index)?;
                    node.insert::<true>(node.end(), pivot, (!leaf).then_some(branch))?;
                    *self.edit.node_mut(owner)?.key_mut(key_at)? = key;
                    let node = self.edit.node_mut(right)?;
                    node.remove::<false>(first)?;
                    return Some(Removed);
                }
            }
            if let Some((_, key)) = left {
                self.merge(owner, key.widen())?;
            } else if right.is_some() {
                self.merge(owner, at)?;
            } else {
                return None;
            }
            child = parent;
        }
        Some(Removed)
    }

    fn merge(&mut self, parent: Index<'owner, L>, gap: Gap) -> Option<Removed> {
        let node = self.edit.node(parent)?;
        let (right, key_at) = node.right(gap)?;
        let left = node.child(gap).brand();
        let right = right.brand();
        let pivot = node.key(key_at);
        let peer = self.edit.node(right)?.snapshot();
        self.edit.node_mut(left)?.append(pivot, peer)?;
        self.edit.node_mut(parent)?.remove::<true>(key_at)?;
        self.edit.release(right)?;
        Some(Removed)
    }
}

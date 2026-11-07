use crate::conn::stream::{resident, state};
use crate::wire::stream;
use o3::depot::map::resident::Id;

use state::resident::{Table, node};

impl Table {
    pub(in crate::conn) const fn available(&self) -> usize {
        self.core.available()
    }

    pub(in crate::conn) fn key(&self, id: stream::Id) -> Option<resident::Key> {
        self.core.key(Id::mask(id.get())).map(resident::Key)
    }

    pub(in crate::conn) fn id(&self, key: resident::Key) -> Option<stream::Id> {
        self.core.id(key.0).map(stream::Id::from_bounded)
    }

    pub(in crate::conn) fn try_id(
        &self,
        local: resident::Local,
    ) -> Result<stream::Id, resident::Stale> {
        self.try_node(local)?;
        Ok(local.id())
    }

    pub(in crate::conn) fn bound_slot(
        &self,
        local: resident::Local,
    ) -> Result<Option<u32>, resident::Stale> {
        let node = self.try_node(local)?;
        Ok(match node.binding {
            node::BINDING_NONE | node::BINDING_UNBOUND => None,
            slot => Some(slot),
        })
    }

    pub(in crate::conn) fn has_event<const SEND: bool>(&self, key: resident::Key) -> bool {
        self.core
            .get(key.0)
            .is_some_and(|entry| entry.word::<SEND>() & node::EVENT != 0)
    }

    pub(in crate::conn) fn recv_exact(&self, local: resident::Local) -> Option<u32> {
        Self::node_slot::<false>(
            self.core
                .lookup(local.key().0, Id::mask(local.id().get()))?,
        )
    }

    pub(in crate::conn) fn try_slot<const SEND: bool>(
        &self,
        local: resident::Local,
    ) -> Result<Option<u32>, resident::Stale> {
        Ok(Self::node_slot::<SEND>(self.try_node(local)?))
    }

    pub(in crate::conn) fn find<const SEND: bool>(
        &self,
        id: stream::Id,
    ) -> Option<(resident::Key, u32)> {
        let (key, node) = self.core.find(Id::mask(id.get()))?;
        Self::node_slot::<SEND>(node).map(|slot| (resident::Key(key), slot))
    }

    pub(in crate::conn) fn slot<const SEND: bool>(&self, key: resident::Key) -> Option<u32> {
        let node = self.core.get(key.0)?;
        Self::node_slot::<SEND>(node)
    }

    fn node_slot<const SEND: bool>(node: &node::Node) -> Option<u32> {
        match node.word::<SEND>() & node::SLOT_MASK {
            node::SLOT_NONE | node::SLOT_CLAIMED => None,
            slot => Some(slot),
        }
    }

    fn try_node(&self, local: resident::Local) -> Result<&node::Node, resident::Stale> {
        self.core
            .lookup(local.key().0, Id::mask(local.id().get()))
            .ok_or_else(|| resident::Stale::new(local, self.key(local.id())))
    }
}

use o3::depot::arena::slab::external::shared;

use crate::driver::{route, route::table::Key};

pub struct Current<'a, T, Tag: route::Tag>(
    pub(in crate::driver::route) shared::Borrowed<'a, T, route::Epoch, Tag>,
);

impl<'a, T, Tag: route::Tag> Current<'a, T, Tag> {
    pub fn key(&self) -> Key<Tag> {
        Key::new(self.0.key())
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.0.get_mut()
    }

    pub fn take(self) -> T {
        self.0.take()
    }

    pub fn discard(self) {
        self.0.discard();
    }
}

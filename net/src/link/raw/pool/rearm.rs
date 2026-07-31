use dope_core::driver::token::{Epoch, SlotIndex, Token};
use o3::collections::FixedQueue;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::link) struct RearmToken(Token);

#[derive(Clone, Copy)]
#[repr(transparent)]
struct RearmIndex(SlotIndex);

pub(super) struct Rearm<const ID: u8> {
    pending: FixedQueue<RearmIndex>,
    epochs: Box<[Option<Epoch>]>,
}

impl<const ID: u8> Rearm<ID> {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            pending: FixedQueue::with_capacity(capacity),
            epochs: vec![None; capacity].into_boxed_slice(),
        }
    }

    pub(super) fn bind(&self, token: Token) -> Option<RearmToken> {
        (token.route() == ID
            && token.kind() == 0
            && token.epoch().is_some()
            && (token.slot().raw() as usize) < self.epochs.len())
        .then_some(RearmToken(token))
    }

    pub(super) fn queue(&mut self, token: RearmToken) {
        let index = token.index();
        let epoch = Self::epoch_mut(&mut self.epochs, index);
        if epoch.is_none() {
            let Some(entry) = self.pending.vacant_entry() else {
                unreachable!()
            };
            entry.push_back(index);
        }
        *epoch = token.token().epoch();
    }

    pub(super) fn pop_front(&mut self) -> Option<Token> {
        let index = self.pending.pop_front()?;
        let epoch = Self::epoch_mut(&mut self.epochs, index).take()?;
        Some(Token::new(ID, index.0, epoch))
    }

    pub(super) fn len(&self) -> usize {
        self.pending.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    fn epoch_mut(epochs: &mut [Option<Epoch>], index: RearmIndex) -> &mut Option<Epoch> {
        // SAFETY: `RearmIndex` can only be derived from a token admitted by
        // this private pool's `bind`, which checks this sidecar's capacity.
        unsafe { epochs.get_unchecked_mut(index.0.raw() as usize) }
    }
}

impl RearmToken {
    pub(in crate::link) const fn token(self) -> Token {
        self.0
    }

    const fn index(self) -> RearmIndex {
        RearmIndex(self.0.slot())
    }
}

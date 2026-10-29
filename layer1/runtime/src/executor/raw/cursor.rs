/// Retained remainder selection for one statically sized sibling group.
/// Empty and sufficiently funded visits leave the cursor unchanged.
#[derive(Clone, Copy)]
pub struct Cursor<const N: usize> {
    next: usize,
}

impl<const N: usize> Cursor<N> {
    pub const INITIAL: Self = {
        assert!(N != 0);
        Self { next: 0 }
    };

    pub(super) fn start(&mut self, left: usize) -> Self {
        if left == 0 || left >= N {
            return Self::INITIAL;
        }
        let selected = *self;
        self.next = if self.next + 1 == N { 0 } else { self.next + 1 };
        selected
    }

    /// Priority of this declaration position among the unvisited siblings.
    pub fn rank<const INDEX: usize>(self) -> usize {
        const { assert!(INDEX < N) };
        if self.next <= INDEX { 0 } else { N - self.next }
    }
}

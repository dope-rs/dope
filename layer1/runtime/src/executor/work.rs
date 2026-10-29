use o3::mem::quota;

/// The generated graph for one finite work domain.
pub trait Work {
    type Graph;
}

/// One generated finite work cycle.
pub trait Cycle: Copy + Eq {
    const FIRST: Self;

    fn next(self) -> Self;
}

/// Persistent fair selection over one compile-time cycle.
/// Generated domains omit this type when fewer than two slots exist.
#[repr(transparent)]
pub struct Arbiter<S: Cycle> {
    cursor: S,
}

impl<S: Cycle> Arbiter<S> {
    pub fn new() -> Self {
        Self { cursor: S::FIRST }
    }

    /// Takes the current slot and advances once.
    pub fn take(&mut self) -> S {
        let at = self.cursor;
        self.account(at);
        at
    }

    /// Finds the first ready slot without advancing the cursor.
    pub fn find(&self, mut ready: impl FnMut(S) -> bool) -> Option<S> {
        let start = self.cursor;
        let mut at = start;
        loop {
            if ready(at) {
                return Some(at);
            }
            at = at.next();
            if at == start {
                return None;
            }
        }
    }

    /// Selects the first ready slot at or after the persistent cursor.
    pub fn select(&mut self, mut ready: impl FnMut(S) -> bool) -> Option<S> {
        let at = self.find(&mut ready)?;
        self.account(at);
        Some(at)
    }

    /// Admits an item from the first ready source.
    pub fn admit<T>(
        &mut self,
        mut admit: impl FnMut(S) -> quota::Admission<T>,
    ) -> quota::Admission<T> {
        let start = self.cursor;
        let mut at = start;
        loop {
            match admit(at) {
                quota::Admission::Item(item) => {
                    self.account(at);
                    return quota::Admission::Item(item);
                }
                quota::Admission::Empty => {}
                quota::Admission::Exhausted => return quota::Admission::Exhausted,
            }
            at = at.next();
            if at == start {
                return quota::Admission::Empty;
            }
        }
    }

    /// Advances past work completed as part of another selected transition.
    pub fn account(&mut self, done: S) {
        self.cursor = done.next();
    }
}

impl<S: Cycle> Default for Arbiter<S> {
    fn default() -> Self {
        Self::new()
    }
}

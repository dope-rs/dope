use o3::buffer::{Shared, SnapshotBuf};

const INITIAL_CAPACITY: usize = 16 * 1024;

pub enum ExtendOutcome {
    Ok,
    Overrun,
}

pub struct State<const HEAD_CAP: usize, const HARD_CAP: usize> {
    accumulator: Option<SnapshotBuf<HARD_CAP>>,
    frozen: bool,
    limit: usize,
}

impl<const HEAD_CAP: usize, const HARD_CAP: usize> Default for State<HEAD_CAP, HARD_CAP> {
    fn default() -> Self {
        Self {
            accumulator: None,
            frozen: false,
            limit: HEAD_CAP,
        }
    }
}

impl<const HEAD_CAP: usize, const HARD_CAP: usize> State<HEAD_CAP, HARD_CAP> {
    pub fn freeze(&mut self) {
        self.frozen = true;
    }

    pub fn unfreeze(&mut self) {
        self.frozen = false;
    }

    pub fn is_frozen(&self) -> bool {
        self.frozen
    }

    pub fn is_accumulating(&self) -> bool {
        self.accumulator.is_some()
    }

    pub fn permit_body(&mut self) {
        self.limit = HARD_CAP;
    }

    pub fn restrict_to_head(&mut self) {
        self.limit = HEAD_CAP;
    }

    pub fn clear(&mut self) {
        self.accumulator = None;
    }

    pub fn snapshot(&self) -> Option<Shared> {
        self.accumulator.as_ref()?.snapshot()
    }

    pub fn advance(&mut self, amount: usize) {
        let Some(accumulator) = self.accumulator.as_mut() else {
            return;
        };
        accumulator.advance(amount);
        if accumulator.is_empty() {
            self.accumulator = None;
        } else {
            accumulator.compact();
        }
    }

    fn extend_capped(
        accumulator: &mut SnapshotBuf<HARD_CAP>,
        src: &[u8],
        limit: usize,
    ) -> ExtendOutcome {
        if accumulator.len() + src.len() > limit {
            return ExtendOutcome::Overrun;
        }
        if accumulator.try_extend_from_slice(src).is_ok() {
            ExtendOutcome::Ok
        } else {
            ExtendOutcome::Overrun
        }
    }

    pub fn extend(&mut self, src: &[u8]) -> ExtendOutcome {
        let limit = self.limit;
        Self::extend_capped(
            self.accumulator
                .get_or_insert_with(|| SnapshotBuf::with_capacity(INITIAL_CAPACITY.min(HARD_CAP))),
            src,
            limit,
        )
    }

    pub fn extend_existing(&mut self, src: &[u8]) -> ExtendOutcome {
        let limit = self.limit;
        let Some(accumulator) = self.accumulator.as_mut() else {
            return ExtendOutcome::Ok;
        };
        Self::extend_capped(accumulator, src, limit)
    }

    /// Retains ingress that cannot be dispatched until an outstanding action
    /// or egress backlog completes.
    ///
    /// Unlike [`Self::extend`], this is bounded by the connection-wide hard
    /// cap rather than the current HTTP frame limit: one receive completion may
    /// legally contain the end of that frame followed by pipelined bytes.
    pub fn extend_backlog(&mut self, src: &[u8]) -> ExtendOutcome {
        Self::extend_capped(
            self.accumulator
                .get_or_insert_with(|| SnapshotBuf::with_capacity(INITIAL_CAPACITY.min(HARD_CAP))),
            src,
            HARD_CAP,
        )
    }

    pub fn try_reserve_to(&mut self, target: usize) -> bool {
        self.accumulator
            .get_or_insert_with(|| SnapshotBuf::with_capacity(INITIAL_CAPACITY.min(HARD_CAP)))
            .try_reserve_to(target)
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::{ExtendOutcome, State};

    #[test]
    fn body_and_backlog_limits_are_independent_from_the_head_limit() {
        let mut recv = State::<8, 32>::default();
        assert!(matches!(recv.extend(&[0; 8]), ExtendOutcome::Ok));
        assert!(matches!(recv.extend(&[0]), ExtendOutcome::Overrun));

        recv.permit_body();
        assert!(matches!(recv.extend(&[0; 24]), ExtendOutcome::Ok));
        assert!(matches!(recv.extend_backlog(&[0]), ExtendOutcome::Overrun));

        recv.clear();
        recv.restrict_to_head();
        assert!(matches!(recv.extend_backlog(&[0; 32]), ExtendOutcome::Ok));
        assert!(matches!(recv.extend_backlog(&[0]), ExtendOutcome::Overrun));
    }
}

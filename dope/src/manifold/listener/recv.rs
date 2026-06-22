use crate::manifold::buf;

pub enum ExtendOutcome {
    Ok,
    Overrun,
}

pub struct State<const HEAD_CAP: usize, const HARD_CAP: usize> {
    pub accum: Option<buf::Accum<HARD_CAP>>,
    pub frozen: bool,
    limit: usize,
}

impl<const HEAD_CAP: usize, const HARD_CAP: usize> Default for State<HEAD_CAP, HARD_CAP> {
    fn default() -> Self {
        Self {
            accum: None,
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

    pub fn set_body_limit(&mut self, declared: usize) {
        self.limit = declared.min(HARD_CAP).max(HEAD_CAP);
    }

    pub fn reset_limit(&mut self) {
        self.limit = HEAD_CAP;
    }

    fn extend_capped(accum: &mut buf::Accum<HARD_CAP>, src: &[u8], limit: usize) -> ExtendOutcome {
        if accum.len() + src.len() > limit {
            return ExtendOutcome::Overrun;
        }
        if accum.extend(src) {
            ExtendOutcome::Ok
        } else {
            ExtendOutcome::Overrun
        }
    }

    pub fn extend_accum(&mut self, src: &[u8]) -> ExtendOutcome {
        let limit = self.limit;
        Self::extend_capped(self.accum.get_or_insert_with(buf::Accum::new), src, limit)
    }

    pub fn extend_existing(&mut self, src: &[u8]) -> ExtendOutcome {
        let limit = self.limit;
        let Some(accum) = self.accum.as_mut() else {
            return ExtendOutcome::Ok;
        };
        Self::extend_capped(accum, src, limit)
    }

    pub fn reserve_accum(&mut self, target: usize) -> bool {
        self.accum
            .get_or_insert_with(buf::Accum::new)
            .reserve(target)
    }
}

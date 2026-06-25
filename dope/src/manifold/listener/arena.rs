use super::send::Buf;

pub(super) struct Handle(u32);

pub(super) struct Arena {
    bufs: Vec<Box<Buf>>,
    free: Vec<u32>,
    hard_cap: usize,
}

impl Arena {
    pub(super) fn new(hard_cap: usize) -> Self {
        Self {
            bufs: Vec::new(),
            free: Vec::new(),
            hard_cap,
        }
    }

    #[inline(always)]
    pub(super) fn borrow(&mut self) -> Option<Handle> {
        let handle = if let Some(idx) = self.free.pop() {
            Handle(idx)
        } else if self.bufs.len() >= self.hard_cap {
            return None;
        } else {
            let idx = self.bufs.len() as u32;
            self.bufs.push(Box::new(Buf::default()));
            Handle(idx)
        };
        crate::memstats::arena_observe(self.bufs.len() - self.free.len());
        Some(handle)
    }

    #[inline(always)]
    pub(super) fn release(&mut self, handle: Handle) {
        self.free.push(handle.0);
    }

    #[inline(always)]
    pub(super) fn slice(&mut self, handle: &Handle) -> &mut [u8] {
        self.bufs[handle.0 as usize].as_mut_slice()
    }

    #[cfg(test)]
    fn live(&self) -> usize {
        self.bufs.len() - self.free.len()
    }

    #[cfg(test)]
    fn free_len(&self) -> usize {
        self.free.len()
    }
}

#[cfg(test)]
mod tests {
    use super::Arena;

    // C1: releasing a handle on send-complete returns the buffer to the free
    // list, so the next borrow reuses it instead of allocating. This is the
    // free-list invariant the listener's send-complete path relies on (it calls
    // `release` once the SEND CQE is observed, gated by `!is_send_inflight()`).
    #[test]
    fn release_returns_buffer_to_free_list() {
        let mut a = Arena::new(8);
        let h0 = a.borrow().expect("first borrow");
        assert_eq!(a.live(), 1);
        assert_eq!(a.free_len(), 0);

        // Simulate the send-complete return site.
        a.release(h0);
        assert_eq!(a.live(), 0);
        assert_eq!(a.free_len(), 1);

        // Next borrow reuses the freed slot: no growth (bufs stays at 1).
        let _h1 = a.borrow().expect("reuse borrow");
        assert_eq!(a.bufs.len(), 1, "freed buffer must be reused, not grown");
        assert_eq!(a.live(), 1);
    }

    // C1: returning on send-complete bounds live buffers to concurrent in-flight
    // writers, not live conns. N borrow/release cycles (one writer at a time)
    // never grow the pool past 1, even across many "connections".
    #[test]
    fn serial_writers_reuse_single_buffer() {
        let mut a = Arena::new(1024);
        for _ in 0..1000 {
            let h = a.borrow().expect("borrow");
            a.release(h);
        }
        assert_eq!(a.bufs.len(), 1, "serial writers must share one buffer");
        assert_eq!(a.live(), 0);
    }

    // The hard cap still guarantees a buffer per concurrent writer up to max_conn:
    // borrow returns None only when every cap slot is simultaneously live.
    #[test]
    fn hard_cap_covers_concurrent_writers() {
        let mut a = Arena::new(2);
        let h0 = a.borrow().expect("0");
        // _h1 is never released: it keeps the second cap slot occupied for the
        // duration of the test so the cap-reached assertion below is meaningful.
        let _h1 = a.borrow().expect("1");
        assert!(a.borrow().is_none(), "cap reached -> None");
        a.release(h0);
        assert!(a.borrow().is_some(), "freed slot reusable after release");
    }
}

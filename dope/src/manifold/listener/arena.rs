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
        if let Some(idx) = self.free.pop() {
            return Some(Handle(idx));
        }
        if self.bufs.len() >= self.hard_cap {
            return None;
        }
        let idx = self.bufs.len() as u32;
        self.bufs.push(Box::new(Buf::default()));
        Some(Handle(idx))
    }

    #[inline(always)]
    pub(super) fn release(&mut self, handle: Handle) {
        self.free.push(handle.0);
    }

    #[inline(always)]
    pub(super) fn slice(&mut self, handle: &Handle) -> &mut [u8] {
        self.bufs[handle.0 as usize].as_mut_slice()
    }
}

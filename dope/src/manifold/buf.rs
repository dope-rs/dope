use o3::buffer::{RawMut, Shared};

const INIT_CAP: usize = 16 * 1024;

trait ByteCopy {
    fn copy_into(&mut self, off: usize, src: &[u8]);
    fn fill_from(&mut self, src: &RawMut, src_off: usize, len: usize);
    fn shift_down(&mut self, src_off: usize, len: usize);
}

impl ByteCopy for RawMut {
    fn copy_into(&mut self, off: usize, src: &[u8]) {
        let n = src.len();
        debug_assert!(off + n <= self.capacity() as usize);
        // SAFETY: off+n within capacity (debug_assert above); src and dst are disjoint.
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), self.data_mut_ptr().add(off), n);
        }
    }

    fn fill_from(&mut self, src: &RawMut, src_off: usize, len: usize) {
        debug_assert!(src_off + len <= src.capacity() as usize);
        debug_assert!(len <= self.capacity() as usize);
        // SAFETY: src_off+len within src cap and len within dst cap; src/dst are distinct RawMut.
        unsafe {
            std::ptr::copy_nonoverlapping(src.data_ptr().add(src_off), self.data_mut_ptr(), len);
        }
    }

    fn shift_down(&mut self, src_off: usize, len: usize) {
        debug_assert!(src_off + len <= self.capacity() as usize);
        // SAFETY: src_off+len within capacity; copy (not copy_nonoverlapping) handles overlap.
        unsafe {
            let base = self.data_mut_ptr();
            std::ptr::copy(base.add(src_off), base, len);
        }
    }
}

pub struct Accum<const HARD_CAP: usize> {
    buf: RawMut,
    cap: u32,
    head: u32,
    tail: u32,
}

impl<const HARD_CAP: usize> Accum<HARD_CAP> {
    pub fn new() -> Self {
        const {
            assert!(HARD_CAP <= u32::MAX as usize, "HARD_CAP must fit u32");
            assert!(HARD_CAP >= INIT_CAP, "HARD_CAP must be >= INIT_CAP");
        }
        let cap = INIT_CAP.min(HARD_CAP);
        Self {
            buf: RawMut::with_capacity(cap),
            cap: cap as u32,
            head: 0,
            tail: 0,
        }
    }

    fn append(&mut self, src: &[u8]) {
        let head = self.head as usize;
        let n = src.len();
        if n > 0 {
            self.buf.ensure_unique_for_mutate(head);
            self.buf.copy_into(head, src);
            self.head = (head + n) as u32;
        }
    }

    #[cold]
    fn grow(&mut self, need: usize) -> bool {
        let unparsed = (self.head - self.tail) as usize;
        let mut new_cap = self.cap as usize;
        while new_cap < unparsed + need {
            if new_cap >= HARD_CAP {
                return false;
            }
            new_cap = (new_cap * 2).min(HARD_CAP);
        }
        self.realloc(new_cap);
        true
    }

    fn realloc(&mut self, new_cap: usize) {
        let unparsed = (self.head - self.tail) as usize;
        let mut fresh = RawMut::with_capacity(new_cap);
        if unparsed > 0 {
            fresh.fill_from(&self.buf, self.tail as usize, unparsed);
        }
        self.buf = fresh;
        self.cap = new_cap as u32;
        self.head = unparsed as u32;
        self.tail = 0;
    }

    #[must_use = "false signals the hard cap was hit: caller must treat it as abuse"]
    pub fn extend(&mut self, src: &[u8]) -> bool {
        if (self.cap - self.head) as usize >= src.len() {
            self.append(src);
            return true;
        }
        self.compact();
        if (self.cap - self.head) as usize >= src.len() {
            self.append(src);
            return true;
        }
        if !self.grow(src.len()) {
            return false;
        }
        self.append(src);
        true
    }

    #[must_use = "false signals the hard cap was hit: caller must treat it as abuse"]
    pub fn reserve(&mut self, target: usize) -> bool {
        if target > HARD_CAP {
            return false;
        }
        if (self.cap as usize) >= target {
            return true;
        }
        self.realloc(target);
        true
    }

    pub fn peek(&self) -> Option<Shared> {
        let t = self.tail;
        let h = self.head;
        if h <= t {
            return None;
        }
        Some(Shared::from_raw_range(self.buf.share(), t, h - t))
    }

    pub fn is_empty(&self) -> bool {
        self.head <= self.tail
    }

    pub fn len(&self) -> usize {
        (self.head - self.tail) as usize
    }

    pub fn advance(&mut self, n: usize) {
        let t = self.tail as usize + n;
        debug_assert!(t <= self.head as usize);
        self.tail = t as u32;
    }

    pub fn compact(&mut self) {
        let t = self.tail as usize;
        let h = self.head as usize;
        if t == 0 {
            return;
        }
        if t >= h {
            self.head = 0;
            self.tail = 0;
            return;
        }
        let unparsed = h - t;
        self.buf.ensure_unique_for_mutate(h);
        self.buf.shift_down(t, unparsed);
        self.head = unparsed as u32;
        self.tail = 0;
    }
}

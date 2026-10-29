use std::{
    hash::{self, Hasher as _},
    mem,
};

use o3::base::marker::ThreadBound;

#[derive(Clone, Copy)]
pub struct Sip<const C: usize, const D: usize> {
    length: usize,
    state: [u64; 4],
    tail: u64,
    ntail: usize,
    _thread: ThreadBound,
}

impl<const C: usize, const D: usize> Sip<C, D> {
    pub(super) fn new(k0: u64, k1: u64) -> Self {
        Self {
            length: 0,
            state: [
                k0 ^ 0x736f_6d65_7073_6575,
                k1 ^ 0x646f_7261_6e64_6f6d,
                k0 ^ 0x6c79_6765_6e65_7261,
                k1 ^ 0x7465_6462_7974_6573,
            ],
            tail: 0,
            ntail: 0,
            _thread: ThreadBound::NEW,
        }
    }

    fn round(state: &mut [u64; 4]) {
        let [v0, v1, v2, v3] = state;
        *v0 = v0.wrapping_add(*v1);
        *v1 = v1.rotate_left(13) ^ *v0;
        *v0 = v0.rotate_left(32);
        *v2 = v2.wrapping_add(*v3);
        *v3 = v3.rotate_left(16) ^ *v2;
        *v0 = v0.wrapping_add(*v3);
        *v3 = v3.rotate_left(21) ^ *v0;
        *v2 = v2.wrapping_add(*v1);
        *v1 = v1.rotate_left(17) ^ *v2;
        *v2 = v2.rotate_left(32);
    }

    fn rounds(state: &mut [u64; 4], count: usize) {
        for _ in 0..count {
            Self::round(state);
        }
    }

    fn compress(state: &mut [u64; 4], word: u64) {
        state[3] ^= word;
        Self::rounds(state, C);
        state[0] ^= word;
    }

    fn pack(mut bytes: &[u8]) -> u64 {
        let mut word = 0;
        let mut shift = 0;
        if let Some((chunk, rest)) = bytes.split_first_chunk::<4>() {
            word = u64::from(u32::from_le_bytes(*chunk));
            bytes = rest;
            shift = 32;
        }
        if let Some((chunk, rest)) = bytes.split_first_chunk::<2>() {
            word |= u64::from(u16::from_le_bytes(*chunk)) << shift;
            bytes = rest;
            shift += 16;
        }
        if let Some(&byte) = bytes.first() {
            word |= u64::from(byte) << shift;
        }
        word
    }

    fn blocks<'a>(state: &mut [u64; 4], bytes: &'a [u8]) -> &'a [u8] {
        let (chunks, rest) = bytes.as_chunks::<8>();
        for chunk in chunks {
            Self::compress(state, u64::from_le_bytes(*chunk));
        }
        rest
    }

    fn short_write(&mut self, size: usize, value: u64) {
        self.length = self.length.wrapping_add(size);
        let needed = 8 - self.ntail;
        self.tail |= value << (8 * self.ntail);
        if size < needed {
            self.ntail += size;
        } else {
            Self::compress(&mut self.state, self.tail);
            self.ntail = size - needed;
            self.tail = if needed < 8 { value >> (8 * needed) } else { 0 };
        }
    }

    pub(super) fn hash(mut self, bytes: &[u8]) -> u64 {
        let rest = Self::blocks(&mut self.state, bytes);
        self.length = bytes.len();
        self.tail = Self::pack(rest);
        self.finish()
    }
}

impl<const C: usize, const D: usize> hash::Hasher for Sip<C, D> {
    fn finish(&self) -> u64 {
        let mut state = self.state;
        let end = (((self.length as u64) & 0xff) << 56) | self.tail;
        Self::compress(&mut state, end);
        state[2] ^= 0xff;
        Self::rounds(&mut state, D);
        state[0] ^ state[1] ^ state[2] ^ state[3]
    }

    fn write(&mut self, mut bytes: &[u8]) {
        self.length = self.length.wrapping_add(bytes.len());
        if self.ntail != 0 {
            let take = bytes.len().min(8 - self.ntail);
            self.tail |= Self::pack(&bytes[..take]) << (8 * self.ntail);
            self.ntail += take;
            bytes = &bytes[take..];
            if self.ntail < 8 {
                return;
            }
            Self::compress(&mut self.state, self.tail);
            self.ntail = 0;
            self.tail = 0;
        }

        let rest = Self::blocks(&mut self.state, bytes);
        self.tail = Self::pack(rest);
        self.ntail = rest.len();
    }

    fn write_u8(&mut self, value: u8) {
        self.short_write(1, u64::from(value));
    }

    fn write_u16(&mut self, value: u16) {
        self.short_write(2, u64::from(value.to_le()));
    }

    fn write_u32(&mut self, value: u32) {
        self.short_write(4, u64::from(value.to_le()));
    }

    fn write_u64(&mut self, value: u64) {
        self.short_write(8, value.to_le());
    }

    fn write_usize(&mut self, value: usize) {
        self.short_write(mem::size_of::<usize>(), value.to_le() as u64);
    }
}

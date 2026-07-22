use std::hash::BuildHasher;
use std::io;

use dope_core::backend::Backend;
use dope_core::platform::Platform;
use siphasher::sip::SipHasher13;

pub mod domain {
    pub const ACCEPT: u64 = u64::from_be_bytes(*b"\0\0accept");
    pub const BACKOFF: u64 = u64::from_be_bytes(*b"\0backoff");
    pub const CACHE: u64 = u64::from_be_bytes(*b"\0\0\0cache");
}
use o3::marker::ThreadBound;

#[derive(Clone, Copy)]
pub struct Seed {
    words: [u64; 2],
}

impl Seed {
    pub fn random() -> io::Result<Self> {
        Ok(Self::new(Backend::entropy()?))
    }

    pub const fn new(words: [u64; 2]) -> Self {
        Self { words }
    }

    pub fn derive(self, domain: u64) -> Self {
        Self {
            words: [mix(self.words[0] ^ domain), mix(self.words[1] ^ !domain)],
        }
    }

    pub const fn state(self) -> State {
        State {
            words: self.words,
            _thread: ThreadBound::NEW,
        }
    }
}

#[derive(Clone, Copy)]
pub struct State {
    words: [u64; 2],
    _thread: ThreadBound,
}

impl BuildHasher for State {
    type Hasher = SipHasher13;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        SipHasher13::new_with_keys(self.words[0], self.words[1])
    }
}

fn mix(mut word: u64) -> u64 {
    word = word.wrapping_add(0x9e37_79b9_7f4a_7c15);
    word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    word ^ (word >> 31)
}

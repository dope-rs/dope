use std::io;

use dope_core::platform;
use o3::cell::brand;

use crate::random;

/// Unbranded process or worker seed. It never crosses the runtime API.
#[derive(Clone, Copy)]
pub(crate) struct Seed {
    words: [u64; 2],
}

impl Seed {
    pub(crate) fn random() -> io::Result<Self> {
        Ok(Self::new(platform::Entropy::acquire()?.into_words()))
    }

    const fn new(words: [u64; 2]) -> Self {
        Self { words }
    }

    pub(crate) fn derive(self, domain: u64) -> Self {
        Self {
            words: [
                Self::mix(self.words[0] ^ domain),
                Self::mix(self.words[1] ^ !domain),
            ],
        }
    }

    pub(crate) fn bind<'d>(
        self,
        _token: &brand::Token<'d>,
        domain: random::Domain,
    ) -> random::HashState<'d> {
        random::HashState::new(self.derive(domain.get()).words)
    }

    fn mix(mut word: u64) -> u64 {
        word = word.wrapping_add(0x9e37_79b9_7f4a_7c15);
        word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        word ^ (word >> 31)
    }
}

const _: () = assert!(std::mem::size_of::<Seed>() == 2 * std::mem::size_of::<u64>());

//! Owned SHA-256 hashing and key derivation through the existing AWS-LC boundary.
//!
//! The audited non-FIPS native build leaves `ENABLE_DATA_INDEPENDENT_TIMING`
//! disabled. Enabling that option requires auditing the DIT behavior of the
//! separate native HMAC extraction and HKDF expansion calls.

use aws_lc_rs::{digest as hash, hkdf, hmac};
use o3::base::marker::ThreadBound;
use zeroize::Zeroizing;

pub use aws_lc_rs::error::Unspecified as Error;

/// A SHA-256 pseudorandom key owned by its constructing thread.
#[derive(Debug)]
pub struct Prk {
    inner: hkdf::Prk,
    _bound: ThreadBound,
}

impl Prk {
    /// Extracts from a fixed-size secret; HMAC admits salts of any length.
    pub fn try_extract(salt: &[u8], secret: &[u8; 32]) -> Result<Self, Error> {
        // SHA-256 selects a supported, non-XOF descriptor. Its native
        // Init/Update callbacks always succeed, including the long-salt
        // hashing branch, and Final retains the admitted 32-byte digest.
        // The HMAC contexts are flat; their clone is an infallible memcpy.
        let key = hmac::Key::new(hmac::HMAC_SHA256, salt);
        let mut material = Zeroizing::new([0; 32]);
        hmac::sign_to_buffer(&key, secret, &mut material[..])?;
        Ok(Self {
            // The fixed 32-byte input fits the upstream 64-byte PRK storage.
            // This constructor selects its owned, zeroizing Expand variant.
            inner: hkdf::Prk::new_less_safe(hkdf::HKDF_SHA256, &material[..]),
            _bound: ThreadBound::NEW,
        })
    }

    /// Expands with the exact context bytes and a fixed 32-byte output.
    ///
    /// At most 80 context bytes keep the upstream concatenation on the stack.
    pub fn try_expand<const N: usize>(
        &self,
        info: &[u8; N],
        output: &mut [u8; 32],
    ) -> Result<(), Error> {
        const { assert!(N <= 80, "info") };
        self.inner
            .expand(&[info.as_slice()], hkdf::HKDF_SHA256)?
            .fill(output)
    }
}

/// Hashes through AWS-LC's one-shot SHA-256 entry point.
#[inline]
pub fn digest(input: &[u8]) -> [u8; 32] {
    let digest = hash::digest(&hash::SHA256, input);
    let Ok(bytes) = <&[u8; 32]>::try_from(digest.as_ref()) else {
        unreachable!("digest");
    };
    *bytes
}

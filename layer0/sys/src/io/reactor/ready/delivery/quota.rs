use crate::io::reactor::ready::store;

/// Runs common maintenance while retaining this exact dispatch reservation.
pub trait Quota<H>: store::Quota {
    fn reclaim(&self, host: &mut H);
}

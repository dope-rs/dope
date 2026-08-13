mod sealed;

pub(super) use sealed::{Capability, Control};

use crate::io::datagram;

pub(super) const LIMITS: datagram::GsoLimits = datagram::GsoLimits {
    max_bytes: 65535,
    max_segments: 64,
};

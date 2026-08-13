use std::io;

use dope_core::platform::affinity;

/// Runtime-owned proof coupling a successful OS binding to this worker thread.
pub(super) struct Binding {
    bound: affinity::Binding,
    _thread: o3::ThreadBound,
}

impl Binding {
    pub(super) fn bind(cpu: u16) -> io::Result<Self> {
        let bound = affinity::Binding::bind(cpu)?;
        Ok(Self {
            bound,
            _thread: o3::ThreadBound::NEW,
        })
    }

    pub(super) fn cpu(&self) -> u16 {
        self.bound.cpu()
    }
}

const _: () = {
    assert!(std::mem::size_of::<Binding>() == std::mem::size_of::<affinity::Binding>());
    assert!(std::mem::align_of::<Binding>() == std::mem::align_of::<affinity::Binding>());
};

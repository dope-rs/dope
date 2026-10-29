use dope_core::platform::wake;

/// Permission to release one group of scripted peers exactly once.
#[must_use = "fire the permission at the scenario's release point"]
#[repr(transparent)]
pub struct Release(pub(super) wake::Notify);

impl Release {
    pub fn try_fire(self) -> Result<(), wake::NotifyError> {
        self.0.notify()
    }
}

const _: () = {
    assert!(size_of::<Release>() == size_of::<wake::Notify>());
    assert!(size_of::<Option<Release>>() == size_of::<Option<wake::Notify>>());
};

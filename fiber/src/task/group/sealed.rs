use core::pin;

pub(crate) struct Binding<'a, 'd> {
    binding: pin::Pin<&'a crate::raw::Binding<'d>>,
}

impl<'a, 'd> Binding<'a, 'd> {
    pub(crate) const fn new(binding: pin::Pin<&'a crate::raw::Binding<'d>>) -> Self {
        Self { binding }
    }
}

// SAFETY: Group structurally pins every Binding and drops the binding array
// before its pinned ready queue. Every completion explicitly unbinds first.
unsafe impl<'a, 'd> crate::raw::StableBindingSource<'a, 'd> for Binding<'a, 'd> {
    fn context(self) -> pin::Pin<&'a crate::raw::Binding<'d>> {
        self.binding
    }
}

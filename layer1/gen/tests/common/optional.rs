use std::{cell::Cell, option, pin::Pin};

#[pin_project::pin_project]
pub struct Option<T> {
    #[pin]
    pub value: option::Option<T>,
    pub present: Cell<bool>,
}

impl<T> Option<T> {
    pub fn as_pin_mut(self: Pin<&mut Self>) -> option::Option<Pin<&mut T>> {
        let this = self.project();
        if this.present.replace(false) {
            this.value.as_pin_mut()
        } else {
            None
        }
    }

    pub fn as_pin_ref(self: Pin<&Self>) -> option::Option<Pin<&T>> {
        let this = self.project_ref();
        if this.present.get() {
            this.value.as_pin_ref()
        } else {
            None
        }
    }
}

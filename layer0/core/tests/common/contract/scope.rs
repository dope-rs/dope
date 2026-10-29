pub struct Scope<F: FnMut()>(pub F);

impl<F: FnMut()> Drop for Scope<F> {
    fn drop(&mut self) {
        self.0();
    }
}

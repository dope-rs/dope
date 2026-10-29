#[must_use = "a classified descriptor retirement must be applied by its backend"]
#[derive(Debug)]
pub enum Kind<C, R> {
    Close(C),
    Retire(R),
}

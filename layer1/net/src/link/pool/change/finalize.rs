pub trait Finalize<T> {
    type Output;

    fn finalize(self, proof: T) -> Self::Output;
}

impl<T> Finalize<T> for () {
    type Output = T;

    fn finalize(self, proof: T) -> Self::Output {
        proof
    }
}

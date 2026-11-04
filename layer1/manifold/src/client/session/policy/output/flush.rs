use core::convert::Infallible;

use o3::base::Call;

/// A flush callback retaining its concrete closure and exact input contract.
pub enum Flush<F = Infallible, C = Infallible> {
    Ready(F),
    Context(C),
}

impl<O, X, I, F: Call<O>, C: Call<(O, X, I)>> Call<(O, X, I)> for Flush<F, C> {
    fn call(self, (owner, context, input): (O, X, I)) {
        match self {
            Self::Ready(call) => call.call(owner),
            Self::Context(call) => call.call((owner, context, input)),
        }
    }
}

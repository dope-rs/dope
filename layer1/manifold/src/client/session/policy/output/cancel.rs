use core::convert::Infallible;

use o3::base::Call;

/// Cancellation transfers the exact unqueued owner to its concrete releaser.
pub enum Cancel<R = Infallible, O = Infallible, C = Infallible, D = Infallible, F = Infallible> {
    Release(R),
    Owner(O),
    Context(C),
    Ready(D, F),
}

impl<V, P, T, R: Call<P>, O: Call<(V, P)>, C: Call<(V, P, T)>, D: Call<P>, F: Call<V>>
    Call<(V, P, T)> for Cancel<R, O, C, D, F>
{
    fn call(self, (owner, payload, token): (V, P, T)) {
        match self {
            Self::Release(call) => call.call(payload),
            Self::Owner(call) => call.call((owner, payload)),
            Self::Context(call) => call.call((owner, payload, token)),
            Self::Ready(release, call) => {
                release.call(payload);
                call.call(owner);
            }
        }
    }
}

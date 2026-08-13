use dope_net::wire;

pub struct Frame<'d>(pub(super) wire::RetainedBytes<'d>);

impl AsRef<[u8]> for Frame<'_> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

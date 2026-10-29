use o3::base::cell::region;

/// A fresh storage identity, consumed exactly once by its owning storage.
pub struct Brand<'s> {
    token: region::Token<region::Identity<'s>>,
}

impl Brand<'_> {
    pub fn with<R>(run: impl for<'s> FnOnce(Brand<'s>) -> R) -> R {
        region::Token::scope(|token| run(Brand { token }))
    }
}

impl<'s> Brand<'s> {
    pub(super) fn into_token(self) -> region::Token<region::Identity<'s>> {
        self.token
    }
}

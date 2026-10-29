#[derive(Debug)]
pub struct Parts<W, F, P, R, B> {
    pub retries: W,
    pub files: F,
    pub poll: P,
    pub reads: R,
    pub recv: B,
}

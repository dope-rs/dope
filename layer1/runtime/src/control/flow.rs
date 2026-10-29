/// Whether a bounded application controller has another transition ready.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    Idle,
    More,
}

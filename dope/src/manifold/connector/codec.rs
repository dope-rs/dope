use o3::buffer::Shared;

pub trait Codec {
    type Head;
    type ParseState: Default;

    fn parse(&self, state: &mut Self::ParseState, buf: &Shared) -> Option<(Self::Head, usize)>;

    fn finish(&self, state: &mut Self::ParseState, remaining: Shared) -> Option<Self::Head> {
        let _ = (state, remaining);
        None
    }
}

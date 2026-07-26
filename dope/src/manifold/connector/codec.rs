use o3::buffer::Shared;

pub trait Codec {
    type Head;
    type ParseState: Default;

    fn parse(&self, state: &mut Self::ParseState, buf: &Shared) -> Option<(Self::Head, usize)>;
}

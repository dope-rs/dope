use crate::{io::reactor::complete::flags::Flow, memory::buffer};

pub trait Meta: Copy + Eq {
    type Buffer: buffer::Index;

    fn buffer(self) -> Option<Self::Buffer>;
    fn flow(self) -> Flow;
}

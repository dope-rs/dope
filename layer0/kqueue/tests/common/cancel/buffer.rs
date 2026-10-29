use o3::buffer;

pub enum Buffer {}

impl dope_sys::Buffer for Buffer {
    fn output(&mut self) -> buffer::raw::Output<'_> {
        match *self {}
    }
    fn len(&self) -> usize {
        match *self {}
    }
    fn recycle(self) {
        match self {}
    }
}

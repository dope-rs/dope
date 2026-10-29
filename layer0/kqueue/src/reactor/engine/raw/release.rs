use crate::reactor::{
    Engine,
    engine::{buffer, engine, ready},
};

impl<C: ready::Driver> buffer::raw::Release<<Self as engine::Buffer>::Token> for Engine<C> {
    unsafe fn release(&mut self, buffer: <Self as engine::Buffer>::Token) {
        let _ = buffer::Recycle::recycle(buffer);
    }
}

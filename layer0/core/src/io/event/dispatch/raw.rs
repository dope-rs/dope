use o3::mem::slot;

use crate::io;

use super::Member;

#[repr(C)]
struct Payload<T> {
    tag: u8,
    value: T,
}

impl<'event, 'd, T> Member<'event, 'd, T> {
    pub fn take(self) -> (T, slot::Vacant<'event, io::Event<'d>>) {
        let mut vacant = self.dispatch.event.into_uninit();
        let payload = vacant.get_mut().as_mut_ptr().cast::<Payload<T>>();
        // SAFETY: selection proves this complete payload of the repr(u8) variant.
        // The fresh pointer is read once; the vacant owner cannot drop Event.
        let value = unsafe { (&raw const (*payload).value).read() };
        (value, vacant)
    }
}

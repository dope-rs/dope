use dope_sys::io::socket::route::selector::Selector;

pub const SELECTOR: Selector = Selector::fixed::<0, 0x80, 1, 10, 5, 9, 6, 16>();

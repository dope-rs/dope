/// Native support for exact provided-buffer exhaustion registrations.
pub enum Support {
    Available,
}

const _: () = {
    assert!(std::mem::size_of::<Support>() == 0);
    assert!(std::mem::align_of::<Support>() == 1);
};

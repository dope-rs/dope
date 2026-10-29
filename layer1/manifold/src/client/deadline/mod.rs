pub(in crate::client) mod book;

/// Which connector deadline retired a transport connection.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(u8)]
pub enum Kind {
    Connect,
    Inbound,
    Send,
    Lifetime,
}

#[derive(Clone, Copy)]
pub(in crate::client) enum Fixed {
    Connect,
    Send,
    Lifetime,
}

impl Fixed {
    pub(in crate::client) const COUNT: usize = 3;
    pub(in crate::client) const ALL: [Self; Self::COUNT] =
        [Self::Connect, Self::Send, Self::Lifetime];

    pub(in crate::client) const fn index(self) -> usize {
        self as usize
    }

    pub(in crate::client) const fn kind(self) -> Kind {
        match self {
            Self::Connect => Kind::Connect,
            Self::Send => Kind::Send,
            Self::Lifetime => Kind::Lifetime,
        }
    }
}

const _: () = assert!(std::mem::size_of::<Kind>() == 1);
const _: () = assert!(std::mem::size_of::<Fixed>() == 1);

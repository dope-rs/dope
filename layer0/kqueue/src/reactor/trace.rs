use dope_sys::io::reactor::complete::Capture;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Trace<E, S, F> {
    Operation {
        echo: E,
    },
    Creation {
        echo: E,
        queued: S,
        resolved: Option<S>,
    },
    Receive {
        echo: E,
        len: usize,
        flow: F,
    },
}

impl<E, S, F> Capture<E, S, F> for Trace<E, S, F> {
    fn operation(echo: E) -> Self {
        Self::Operation { echo }
    }

    fn creation(echo: E, queued: S, resolved: Option<S>) -> Self {
        Self::Creation {
            echo,
            queued,
            resolved,
        }
    }

    fn receive(echo: E, len: usize, flow: F) -> Self {
        Self::Receive { echo, len, flow }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Trace<u64, u32, bool>>() == 3 * std::mem::size_of::<usize>());
};

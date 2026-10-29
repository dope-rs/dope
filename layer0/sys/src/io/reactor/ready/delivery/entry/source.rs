use crate::io::reactor::{
    read,
    ready::delivery::entry::{Accepted, Context, Event, Flow, Key},
    retry,
};

pub trait Source {
    type Event<C: Context, B: crate::Buffer>: Event<C, B>
        + read::Complete<Key = Key<C>, Flow = Flow<C>, Accepted = Accepted<C>, Buffer = B>
        + retry::Complete<C::Writer>;
}

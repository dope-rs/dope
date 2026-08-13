use dope_net::wire;

use crate::receive;

pub trait Delivery {
    type Value<'input, 'd, W: wire::Wire>
    where
        'd: 'input;
}

impl Delivery for receive::Borrowed {
    type Value<'input, 'd, W: wire::Wire>
        = W::RecvBatch<'input>
    where
        'd: 'input;
}

impl Delivery for receive::Retained {
    type Value<'input, 'd, W: wire::Wire>
        = W::RetainedRecv<'d>
    where
        'd: 'input;
}

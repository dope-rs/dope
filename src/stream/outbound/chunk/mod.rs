mod node;
pub(in crate::stream) mod prefix;

pub(crate) use node::Node;

pub(super) type Front<'a, O> = o3::depot::queue::pool::Front<'a, Node<O>, ()>;

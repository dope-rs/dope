use dope_net::link::egress::metadata;
use o3::base::cell::region;

fn reenter<'p, 'd>(
    queue: metadata::Queue<'_, 'p, 'd, &'d [u8]>,
    token: &mut region::Token<region::Identity<'d>>,
) -> Option<&'d [u8]> {
    let front = queue.take_front(token)?;
    drop(queue.take_front(token));
    Some(*front.get())
}

fn main() {}

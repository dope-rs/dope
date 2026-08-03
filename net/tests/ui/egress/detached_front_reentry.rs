use dope_net::link::egress::metadata;
use o3::cell::RegionToken;

fn reenter<'d>(token: &mut RegionToken<'d>, queue: metadata::Queue<'_, 'd, &'static [u8]>) {
    let (_, front) = queue.take_front(token).unwrap();
    queue.try_push_back(token, b"next", 4).unwrap();
    front.release();
}

fn main() {}

use dope_net::link::egress::metadata::batch::Rejected;

fn escape<'a, 'out>(rejected: Rejected<&'a [u8]>) -> Rejected<&'out [u8]> {
    rejected
}

fn main() {}

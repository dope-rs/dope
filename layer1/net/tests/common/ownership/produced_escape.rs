use dope_net::wire::receive::batch::Produced;
fn escape<'id, 'out, B>(output: Produced<'id, B>) -> Produced<'out, B> {
    output
}

fn main() {}

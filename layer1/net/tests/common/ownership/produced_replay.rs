use dope_net::wire::receive::batch::Produced;
fn replay<'id, B>(output: Produced<'id, B>) -> (Produced<'id, B>, Produced<'id, B>) {
    (output, output)
}

fn main() {}

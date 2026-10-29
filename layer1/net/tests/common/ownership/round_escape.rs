use dope_net::link::pool::pending::Round;

fn escape<'p, 'd, 'out>(round: Round<'p, 'd>) -> Round<'out, 'd> {
    round
}

fn main() {}

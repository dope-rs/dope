use dope_runtime::random::HashState;

fn escape<'d, 'owner: 'd>(state: HashState<'d>) -> HashState<'owner> {
    state
}

fn main() {}

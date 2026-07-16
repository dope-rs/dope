extern crate dope;
use dope::manifold::listener::recv::{ExtendOutcome, State};

const HEAD: usize = 64 * 1024;
const BODY: usize = 4 * 1024 * 1024;

fn state() -> State<HEAD, BODY> {
    State::default()
}

fn feed_until_overrun(
    state: &mut State<HEAD, BODY>,
    chunk_len: usize,
    max_iters: usize,
) -> Option<usize> {
    let chunk = vec![0u8; chunk_len];
    let mut total = 0usize;
    for _ in 0..max_iters {
        total += chunk.len();
        if matches!(state.extend(&chunk), ExtendOutcome::Overrun) {
            return Some(total);
        }
    }
    None
}

fn feed_no_overrun(state: &mut State<HEAD, BODY>, chunk_len: usize, iters: usize) {
    let chunk = vec![0u8; chunk_len];
    for _ in 0..iters {
        assert!(
            !matches!(state.extend(&chunk), ExtendOutcome::Overrun),
            "must accumulate without overrun"
        );
    }
}

#[test]
fn pre_parse_head_is_capped_at_head_cap() {
    let mut state = state();
    let chunk_len = 16 * 1024;
    let total =
        feed_until_overrun(&mut state, chunk_len, 8).expect("never-completing head must overrun");
    assert!(
        total <= HEAD + chunk_len,
        "head overran far past HEAD_CAP ({total} bytes); body cap leaked into pre-parse phase"
    );
}

#[test]
fn declared_body_unlocks_large_cap() {
    let mut state = state();
    state.permit_body();
    feed_no_overrun(&mut state, 256 * 1024, 8);
}

#[test]
fn restrict_to_head_restores_cap_for_next_request() {
    let mut state = state();
    state.permit_body();
    let chunk_len = 256 * 1024;
    feed_no_overrun(&mut state, chunk_len, 1);
    state.clear();
    state.restrict_to_head();
    let total = feed_until_overrun(&mut state, chunk_len, 8)
        .expect("after reset, follow-up request must re-cap at HEAD_CAP");
    assert!(
        total <= HEAD + chunk_len,
        "elevated body limit leaked into next request ({total} bytes)"
    );
}

#[test]
fn body_limit_clamped_to_hard_cap() {
    let mut state = state();
    state.permit_body();
    assert!(
        feed_until_overrun(&mut state, 1024 * 1024, 8).is_some(),
        "oversized declared body must still overrun at HARD_CAP"
    );
}

use dope::manifold::listener::recv::{ExtendOutcome, State};

const HEAD: usize = 64 * 1024;
const BODY: usize = 4 * 1024 * 1024;

fn overrun(o: ExtendOutcome) -> bool {
    matches!(o, ExtendOutcome::Overrun)
}

#[test]
fn pre_parse_head_is_capped_at_head_cap() {
    let mut s: State<HEAD, BODY> = State::default();
    let chunk = vec![0u8; 16 * 1024];
    let mut total = 0usize;
    let mut hit = false;
    for _ in 0..8 {
        total += chunk.len();
        if overrun(s.extend_accum(&chunk)) {
            hit = true;
            break;
        }
    }
    assert!(hit, "never-completing head must overrun");
    assert!(
        total <= HEAD + chunk.len(),
        "head overran far past HEAD_CAP ({total} bytes); body cap leaked into pre-parse phase"
    );
}

#[test]
fn declared_body_unlocks_large_cap() {
    let mut s: State<HEAD, BODY> = State::default();
    s.set_body_limit(2 * 1024 * 1024);
    let chunk = vec![0u8; 256 * 1024];
    for _ in 0..8 {
        assert!(
            !overrun(s.extend_accum(&chunk)),
            "declared 2MiB body must accumulate past HEAD_CAP"
        );
    }
}

#[test]
fn reset_limit_restores_head_cap_for_next_request() {
    let mut s: State<HEAD, BODY> = State::default();
    s.set_body_limit(2 * 1024 * 1024);
    let chunk = vec![0u8; 256 * 1024];
    assert!(!overrun(s.extend_accum(&chunk)));
    s.accum = None;
    s.reset_limit();
    let mut total = 0usize;
    let mut hit = false;
    for _ in 0..8 {
        total += chunk.len();
        if overrun(s.extend_accum(&chunk)) {
            hit = true;
            break;
        }
    }
    assert!(
        hit,
        "after reset, follow-up request must re-cap at HEAD_CAP"
    );
    assert!(
        total <= HEAD + chunk.len(),
        "elevated body limit leaked into next request ({total} bytes)"
    );
}

#[test]
fn body_limit_clamped_to_hard_cap() {
    let mut s: State<HEAD, BODY> = State::default();
    s.set_body_limit(usize::MAX);
    let chunk = vec![0u8; 1024 * 1024];
    let mut hit = false;
    for _ in 0..8 {
        if overrun(s.extend_accum(&chunk)) {
            hit = true;
            break;
        }
    }
    assert!(
        hit,
        "oversized declared body must still overrun at HARD_CAP"
    );
}

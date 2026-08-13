use dope_net::{link, wire};
use wire::{reclaim, send};

#[test]
fn exact_input_derives_its_plaintext_length() {
    let plain = send::Plain::from_static(b"exact");
    let prepared = send::Prepared::<reclaim::OnComplete>::input(plain);
    let (empty, consumed, close_after) = prepared.inspect();

    assert!(!empty);
    assert_eq!(consumed, 5);
    assert!(!close_after);
}

#[test]
fn exact_completion_is_the_only_plain_conversion() -> Result<(), &'static str> {
    let Some(sent) = send::Sent::try_from_submission(7, 7) else {
        return Err("valid completion size was rejected");
    };
    let Some(completed) = <reclaim::OnComplete as reclaim::Policy>::completed_plain(sent) else {
        return Err("exact completion was rejected");
    };

    assert_eq!(completed.get(), 7);
    assert!(<reclaim::OnSubmit as reclaim::Policy>::completed_plain(sent).is_none());
    assert_eq!(
        std::mem::size_of::<link::Consumed>(),
        std::mem::size_of::<usize>()
    );
    Ok(())
}

#[test]
fn exact_transition_is_terminal_and_empty() {
    let mut storage = ();
    let transition = send::Transition::<reclaim::OnComplete>::completed(send::Storage::from_raw(
        &mut storage,
        9,
    ));
    let (empty, consumed, close_after, availability) = transition.inspect();

    assert!(empty);
    assert_eq!(consumed, 0);
    assert!(!close_after);
    assert_eq!(availability, send::Availability::Unchanged);
}

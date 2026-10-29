pub mod common;

use std::{cell::Cell, num::NonZero};

use dope_kqueue::Kqueue;
use dope_sys::io::reactor::complete::{self, Dequeued, Queue as _, Vacancy as _};

use crate::common::event::{Capacity, Event};

type Queue<'a> = <Kqueue as complete::Source>::Queue<Event<'a>, Capacity>;

#[test]
fn suppression_keeps_owners() {
    let retired = Cell::new(0);
    let key = NonZero::<u8>::MIN;
    let mut queue = Queue::try_new(Capacity(NonZero::<u32>::MIN.saturating_add(2)))
        .expect("completion storage");
    for value in 0..3 {
        queue.vacant().expect("reserved capacity").push(Event {
            key: (value != 2).then_some(key),
            value,
            terminal: false,
            retired: &retired,
        });
    }
    assert!(matches!(
        queue.pop_front(),
        Some(Dequeued::Emit(Event { value: 0, .. }))
    ));
    queue.vacant().expect("wrapped vacancy").push(Event {
        key: Some(key),
        value: 3,
        terminal: true,
        retired: &retired,
    });
    assert!(queue.is_terminal(key));
    queue.suppress(key);
    assert_eq!(retired.get(), 1);
    assert!(queue.is_terminal(key));
    for value in 1..4 {
        let entry = queue.pop_front().expect("retained completion");
        let event = match entry {
            Dequeued::Emit(event) => {
                assert_eq!(value, 2);
                event
            }
            Dequeued::Reclaim(event) => {
                assert_ne!(value, 2);
                event
            }
        };
        assert_eq!(event.value, value);
        assert_eq!(event.key, (value != 2).then_some(key));
        assert_eq!(retired.get(), value);
    }
    assert_eq!(retired.get(), 4);
    assert!(!queue.is_terminal(key));

    queue.vacant().expect("reused capacity").push(Event {
        key: Some(key),
        value: 4,
        terminal: true,
        retired: &retired,
    });
    assert!(matches!(
        queue.pop_front(),
        Some(Dequeued::Emit(Event { value: 4, .. }))
    ));
    assert_eq!(retired.get(), 5);
}

#[test]
fn more_keeps_pending() {
    let retired = Cell::new(0);
    let key = NonZero::<u8>::MIN;
    let mut queue = Queue::try_new(Capacity(NonZero::<u32>::MIN)).expect("completion storage");
    queue.vacant().expect("reserved capacity").push(Event {
        key: Some(key),
        value: 7,
        terminal: false,
        retired: &retired,
    });
    assert!(!queue.is_terminal(key));
    queue.suppress(key);
    assert!(!queue.is_terminal(key));
    assert!(matches!(
        queue.pop_front(),
        Some(Dequeued::Reclaim(Event {
            value: 7,
            terminal: false,
            ..
        }))
    ));
    assert_eq!(retired.get(), 1);
}

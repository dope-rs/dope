extern crate dope;
use o3::cell::{BrandCell, BrandToken};
use std::cell::Cell;
use std::marker::PhantomPinned;

struct Slot {
    inflight: Vec<u8>,
    close_after: bool,
}

struct Aux {
    scratch: Vec<u8>,
}

struct Timer {
    armed: u32,
}

struct Pinned {
    value: Cell<u32>,
    _pin: PhantomPinned,
}

struct Listener {
    pool: Vec<Slot>,
    app: App,
    aux: Aux,
}

struct App {
    seen: usize,
}

impl App {
    fn chunk<'id>(
        &mut self,
        slot: &mut Slot,
        chunk: &[u8],
        aux: &mut Aux,
        timer: &BrandCell<'id, Timer>,
        token: &mut BrandToken<'id>,
    ) -> bool {
        aux.scratch.clear();
        aux.scratch.extend_from_slice(chunk);
        slot.inflight.extend_from_slice(&aux.scratch);
        timer.borrow_mut(token).armed += 1;
        self.seen += 1;
        chunk.ends_with(b"\n")
    }
}

#[test]
fn dispatch_loop_shape_under_split_brands() {
    BrandToken::scope(|mut listener_token| {
        BrandToken::scope(|mut timer_token| {
            let listener = BrandCell::new(Listener {
                pool: vec![
                    Slot {
                        inflight: Vec::new(),
                        close_after: false,
                    },
                    Slot {
                        inflight: Vec::new(),
                        close_after: false,
                    },
                ],
                app: App { seen: 0 },
                aux: Aux {
                    scratch: Vec::new(),
                },
            });
            let timer = BrandCell::new(Timer { armed: 0 });
            let timer_handle = &timer;

            let events: [(usize, &[u8]); 3] = [(0, b"a"), (1, b"b\n"), (0, b"c\n")];
            for (idx, chunk) in events {
                let this = listener.borrow_mut(&mut listener_token);
                let (pool, app, aux) = (&mut this.pool, &mut this.app, &mut this.aux);
                let slot = &mut pool[idx];
                if app.chunk(slot, chunk, aux, timer_handle, &mut timer_token) {
                    slot.close_after = true;
                }
            }

            let this = listener.borrow(&listener_token);
            assert_eq!(this.app.seen, 3);
            assert_eq!(this.pool[0].inflight, b"ac\n");
            assert!(this.pool[0].close_after);
            assert!(this.pool[1].close_after);
            assert_eq!(timer.borrow(&timer_token).armed, 3);
        });
    });
}

#[test]
fn pinned_borrow_keeps_the_value_in_place() {
    BrandToken::scope(|mut token| {
        let cell = BrandCell::new(Pinned {
            value: Cell::new(0),
            _pin: PhantomPinned,
        });
        let cell = std::pin::pin!(cell);
        let first = {
            let value = cell.as_ref().borrow_pin_mut(&mut token);
            value.as_ref().get_ref().value.set(7);
            value.as_ref().get_ref() as *const Pinned
        };
        let value = cell.as_ref().borrow_pin(&token);
        assert_eq!(first, value.get_ref() as *const Pinned);
        assert_eq!(value.get_ref().value.get(), 7);
    });
}

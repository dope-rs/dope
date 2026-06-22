use std::time::{Duration, Instant};

use crate::backend;

const NIL: u32 = u32::MAX;

struct Node {
    deadline: Instant,
    prev: u32,
    next: u32,
}

pub(super) struct IdleSet {
    nodes: Vec<Node>,
    head: u32,
    tail: u32,
    window: Duration,
}

impl IdleSet {
    pub(super) fn new(cap: usize, window: Duration) -> Self {
        let now = Instant::now();
        let nodes = (0..cap)
            .map(|i| Node {
                deadline: now,
                prev: NIL,
                next: i as u32,
            })
            .collect();
        Self {
            nodes,
            head: NIL,
            tail: NIL,
            window,
        }
    }

    fn unlink(&mut self, idx: u32) {
        let (prev, next) = {
            let n = &self.nodes[idx as usize];
            (n.prev, n.next)
        };
        if prev != NIL {
            self.nodes[prev as usize].next = next;
        } else {
            self.head = next;
        }
        if next != NIL {
            self.nodes[next as usize].prev = prev;
        } else {
            self.tail = prev;
        }
        self.nodes[idx as usize].next = idx;
    }

    pub(super) fn arm(&mut self, idx: backend::token::LocalIdx, now: Instant) {
        let raw = idx.raw();
        let i = raw as usize;
        if i >= self.nodes.len() {
            return;
        }
        if self.nodes[i].next != raw {
            self.unlink(raw);
        }
        let deadline = now + self.window;
        let tail = self.tail;
        {
            let node = &mut self.nodes[i];
            node.deadline = deadline;
            node.prev = tail;
            node.next = NIL;
        }
        if tail != NIL {
            self.nodes[tail as usize].next = raw;
        } else {
            self.head = raw;
        }
        self.tail = raw;
    }

    pub(super) fn cancel(&mut self, idx: backend::token::LocalIdx) {
        let raw = idx.raw();
        let i = raw as usize;
        if i >= self.nodes.len() || self.nodes[i].next == raw {
            return;
        }
        self.unlink(raw);
    }

    pub(super) fn pop_expired(&mut self, now: Instant) -> Option<backend::token::LocalIdx> {
        if self.head == NIL {
            return None;
        }
        let h = self.head;
        if self.nodes[h as usize].deadline > now {
            return None;
        }
        self.unlink(h);
        Some(backend::token::LocalIdx::new(h))
    }

    pub(super) fn earliest(&self) -> Option<Instant> {
        (self.head != NIL).then(|| self.nodes[self.head as usize].deadline)
    }
}

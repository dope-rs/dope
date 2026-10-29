use std::{cmp, num};

mod edit;

use edit::Edit;

use crate::link::egress::{
    self,
    load::credit::{
        self,
        wait::store::{NodeView, Nodes},
    },
};

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub(in crate::link::egress::load::credit::wait) struct Index(num::NonZero<u32>);

impl Index {
    /// Reuses the lane admitted by the queue's retained credit pool.
    pub(in crate::link::egress::load::credit::wait) const fn from<'p, 'd>(
        lane: credit::Lane<'_, 'p, 'd, credit::Ready<'p, 'd>>,
    ) -> Self {
        Self(lane.index().position())
    }

    pub(in crate::link::egress::load::credit::wait) const fn offset(self) -> usize {
        (self.0.get() - 1) as usize
    }
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub(in crate::link::egress::load::credit::wait) struct Ticket(u32);

pub(in crate::link::egress::load::credit::wait) struct Issued {
    ticket: Ticket,
    next: Ticket,
}

impl Ticket {
    pub(in crate::link::egress::load::credit::wait) const FIRST: Self = Self(0);

    pub(in crate::link::egress::load::credit::wait) fn issue(
        self,
    ) -> Result<Issued, egress::Tickets> {
        let next = self.0.checked_add(1).ok_or(egress::Tickets)?;
        Ok(Issued {
            ticket: self,
            next: Self(next),
        })
    }
}

impl Issued {
    pub(in crate::link::egress::load::credit::wait) const fn ticket(&self) -> Ticket {
        self.ticket
    }

    pub(in crate::link::egress::load::credit::wait) const fn next(&self) -> Ticket {
        self.next
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub(in crate::link::egress::load::credit) struct Candidate(Index);

impl Candidate {
    pub(in crate::link::egress::load::credit) const fn position(self) -> num::NonZero<u32> {
        self.0.0
    }

    pub(in crate::link::egress::load::credit::wait) const fn offset(self) -> usize {
        self.0.offset()
    }

    pub(in crate::link::egress::load::credit::wait) const fn index(self) -> Index {
        self.0
    }
}

pub(in crate::link::egress::load::credit::wait) struct Waiting {
    index: Index,
}

impl Waiting {
    pub(in crate::link::egress::load::credit::wait) const fn new(index: Index) -> Self {
        Self { index }
    }
}

/// Shared entry demand after this lane's protected credit has been applied.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum Entries {
    Zero,
    One,
    Two,
}

impl Entries {
    fn from(demand: credit::Demand<2>) -> Self {
        match demand.entry_bound().get() {
            0 => Self::Zero,
            1 => Self::One,
            _ => Self::Two,
        }
    }
}

#[derive(Clone, Copy)]
struct Oldest {
    ticket: Ticket,
    lane: Index,
}

impl Oldest {
    fn best(left: Option<Self>, right: Option<Self>) -> Option<Self> {
        match (left, right) {
            (Some(left), Some(right)) => Some(cmp::min_by_key(left, right, |item| item.ticket)),
            (Some(left), None) => Some(left),
            (None, right) => right,
        }
    }

    fn available(oldest: Option<Self>) -> Option<Candidate> {
        oldest.map(|oldest| Candidate(oldest.lane))
    }

    fn at(nodes: &Nodes<'_>, index: Option<Index>) -> Option<Self> {
        index.and_then(|index| nodes.get(index).oldest())
    }

    fn at_view(nodes: &NodeView<'_>, index: Option<Index>) -> Option<Self> {
        index.and_then(|index| nodes.get(index).oldest())
    }

    fn eligible(
        nodes: &NodeView<'_>,
        index: Option<Index>,
        resident: egress::Resident,
    ) -> Option<Self> {
        index.and_then(|index| {
            let current = nodes.get(index);
            if current.resident > resident {
                return Self::eligible(nodes, current.left, resident);
            }
            let left = Self::at_view(nodes, current.left);
            let own = Some(Self {
                ticket: current.ticket,
                lane: index,
            });
            let right = Self::eligible(nodes, current.right, resident);
            Self::best(Self::best(left, own), right)
        })
    }
}

pub(in crate::link::egress::load::credit::wait) struct Node {
    ticket: Ticket,
    oldest: Option<Oldest>,
    left: Option<Index>,
    right: Option<Index>,
    parent: Option<Index>,
    resident: egress::Resident,
    height: u8,
    entries: Entries,
}

impl Node {
    pub(in crate::link::egress::load::credit::wait) const fn idle() -> Self {
        Self {
            ticket: Ticket::FIRST,
            oldest: None,
            left: None,
            right: None,
            parent: None,
            resident: egress::Resident::new(0),
            height: 1,
            entries: Entries::Zero,
        }
    }

    const fn waiting(
        lane: Index,
        resident: egress::Resident,
        entries: Entries,
        ticket: Ticket,
    ) -> Self {
        Self {
            ticket,
            oldest: Some(Oldest { ticket, lane }),
            left: None,
            right: None,
            parent: None,
            resident,
            height: 1,
            entries,
        }
    }

    fn oldest(&self) -> Option<Oldest> {
        self.oldest
    }

    fn height(nodes: &Nodes<'_>, index: Option<Index>) -> u8 {
        match index {
            Some(index) => nodes.get(index).height,
            None => 0,
        }
    }
}

pub(in crate::link::egress::load::credit::wait) struct State {
    zero: Option<Index>,
    one: Option<Index>,
    two: Option<Index>,
}

impl State {
    pub(in crate::link::egress::load::credit::wait) const fn new() -> Self {
        Self {
            zero: None,
            one: None,
            two: None,
        }
    }

    const fn get(&self, entries: Entries) -> Option<Index> {
        match entries {
            Entries::Zero => self.zero,
            Entries::One => self.one,
            Entries::Two => self.two,
        }
    }

    pub(in crate::link::egress::load::credit::wait) const fn is_empty(&self) -> bool {
        self.zero.is_none() && self.one.is_none() && self.two.is_none()
    }

    fn set(&mut self, entries: Entries, root: Option<Index>) {
        match entries {
            Entries::Zero => self.zero = root,
            Entries::One => self.one = root,
            Entries::Two => self.two = root,
        }
    }

    fn oldest_view(&self, nodes: &NodeView<'_>) -> Option<Oldest> {
        let zero = Oldest::at_view(nodes, self.zero);
        let one = Oldest::at_view(nodes, self.one);
        let two = Oldest::at_view(nodes, self.two);
        Oldest::best(Oldest::best(zero, one), two)
    }

    pub(in crate::link::egress::load::credit::wait) fn insert(
        &mut self,
        nodes: &mut Nodes<'_>,
        waiting: Waiting,
        demand: credit::Demand<2>,
        ticket: Ticket,
    ) {
        let node = Node::waiting(
            waiting.index,
            demand.resident(),
            Entries::from(demand),
            ticket,
        );
        self.insert_node(nodes, waiting, node);
    }

    pub(in crate::link::egress::load::credit::wait) fn remove(
        &mut self,
        nodes: &mut Nodes<'_>,
        waiting: Waiting,
    ) {
        let Waiting { index } = waiting;
        let entries = nodes.get(index).entries;
        let root = Edit::new(nodes).remove(self.get(entries), index);
        self.set(entries, root);
    }

    pub(in crate::link::egress::load::credit::wait) fn refresh(
        &mut self,
        nodes: &mut Nodes<'_>,
        waiting: Waiting,
        demand: credit::Demand<2>,
    ) {
        let Waiting { index } = waiting;
        let node = nodes.get(index);
        let ticket = node.ticket;
        let entries = Entries::from(demand);
        if node.entries == entries && node.resident == demand.resident() {
            return;
        }
        self.remove(nodes, Waiting::new(index));
        self.insert_node(
            nodes,
            Waiting::new(index),
            Node::waiting(index, demand.resident(), entries, ticket),
        );
    }

    pub(in crate::link::egress::load::credit::wait) fn oldest(
        &self,
        nodes: &NodeView<'_>,
    ) -> Option<Candidate> {
        Oldest::available(self.oldest_view(nodes))
    }

    pub(in crate::link::egress::load::credit::wait) fn eligible(
        &self,
        nodes: &NodeView<'_>,
        shared: credit::Amount,
    ) -> Option<Candidate> {
        let resident = shared.resident();
        let zero = Oldest::eligible(nodes, self.zero, resident);
        let entries = shared.entries();
        let oldest = if entries.is_empty() {
            zero
        } else if entries == egress::Entries::new(1) {
            Oldest::best(zero, Oldest::eligible(nodes, self.one, resident))
        } else {
            let one = Oldest::eligible(nodes, self.one, resident);
            let two = Oldest::eligible(nodes, self.two, resident);
            Oldest::best(Oldest::best(zero, one), two)
        };
        Oldest::available(oldest)
    }

    fn insert_node(&mut self, nodes: &mut Nodes<'_>, waiting: Waiting, node: Node) {
        let Waiting { index } = waiting;
        let entries = node.entries;
        *nodes.get_mut(index) = node;
        let root = Edit::new(nodes).insert(self.get(entries), index);
        nodes.get_mut(root).parent = None;
        self.set(entries, Some(root));
    }
}
const _: () = {
    assert!(std::mem::size_of::<Index>() == std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<Option<Index>>() == std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<Ticket>() == std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<Issued>() == 2 * std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<Waiting>() == std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<Oldest>() == std::mem::size_of::<u64>());
    assert!(std::mem::size_of::<Option<Oldest>>() == std::mem::size_of::<u64>());
    assert!(std::mem::size_of::<Node>() == 4 * std::mem::size_of::<u64>());
    assert!(std::mem::size_of::<State>() == 3 * std::mem::size_of::<u32>());
};

use std::mem;

use dope_core::driver::schedule::ready;

use crate::link::egress::{
    self, create,
    load::credit::{
        self,
        wait::{
            store::{
                Bypass, Bypassable, Core, Idle, Nodes, Queued, Ready, Records, Selected, State,
                Waiting,
            },
            tree,
        },
    },
    metadata,
};

pub(in crate::link::egress::load::credit) enum Resume<'p> {
    Waiting,
    Queued,
    Granted(credit::Grant<'p>),
}

pub(in crate::link::egress::load::credit) enum Cancelled {
    Idle,
    Busy,
}

pub(in crate::link::egress::load::credit) enum Selection<'a, 'pool, 'p, 'd> {
    Selected(Selected<'a, 'pool, 'p, 'd>),
    NoWaiting,
    Insufficient,
    BypassExhausted,
}

struct Vacant<'a, 'b, 'p, 'd> {
    record: Idle<'a, 'p, 'd>,
    tree: &'a mut tree::State,
    nodes: &'a mut Nodes<'b>,
    ticket: &'a mut tree::Ticket,
    index: tree::Index,
}

impl<'p, 'd> Vacant<'_, '_, 'p, 'd> {
    fn poll(
        self,
        lane: credit::Lane<'_, 'p, 'd, credit::Ready<'p, 'd>>,
        key: ready::Retained<'d>,
        amount: credit::Queued,
    ) -> Result<Resume<'p>, egress::Tickets> {
        let charge = amount.amount();
        let account = lane.account();
        let demand = credit::Demand::queued(account, amount);
        if demand == credit::Demand::<2>::ZERO || (self.tree.is_empty() && lane.is_covered(demand))
        {
            return Ok(Resume::Granted(account.acquire(charge)));
        }
        if self.tree.is_empty() {
            *self.ticket = tree::Ticket::FIRST;
        }
        let issued = self.ticket.issue()?;
        *self.ticket = issued.next();
        let record = self.record.wait(
            self.index,
            Waiting {
                key,
                amount,
                bypasses: 0,
            },
        );
        let proof = record.proof();
        self.tree.insert(self.nodes, proof, demand, issued.ticket());
        Ok(Resume::Queued)
    }
}

pub(in crate::link::egress::load::credit) struct Queue<'p, 'd> {
    store: Core<'p, 'd>,
    tree: tree::State,
    ticket: tree::Ticket,
}

impl<'p, 'd> Queue<'p, 'd> {
    pub(in crate::link::egress::load::credit) fn try_new(
        lanes: metadata::LaneCount,
    ) -> Result<Self, create::Error> {
        const {
            let store = mem::size_of::<Core<'p, 'd>>();
            let roots = mem::size_of::<tree::State>();
            let ticket = mem::size_of::<tree::Ticket>();
            assert!(store == 3 * mem::size_of::<usize>());
            assert!(roots == 3 * mem::size_of::<u32>());
            assert!(ticket == mem::size_of::<u32>());
            assert!(mem::align_of::<Core<'p, 'd>>() == mem::align_of::<usize>());
            assert!(mem::align_of::<tree::State>() == mem::align_of::<u32>());
            assert!(mem::align_of::<tree::Ticket>() == mem::align_of::<u32>());
            assert!(mem::align_of::<Self>() == mem::align_of::<usize>());
            assert!(mem::size_of::<Self>() == store + roots + ticket);
            assert!(mem::size_of::<Self>() == 5 * mem::size_of::<usize>());
        };
        Ok(Self {
            store: Core::try_new(lanes)?,
            tree: tree::State::new(),
            ticket: tree::Ticket::FIRST,
        })
    }

    pub(in crate::link::egress::load::credit) fn is_busy(
        &self,
        lane: credit::Lane<'_, 'p, 'd, credit::Ready<'p, 'd>>,
    ) -> bool {
        self.store.is_busy(lane)
    }

    pub(in crate::link::egress::load::credit) fn has_waiting(&self) -> bool {
        !self.tree.is_empty()
    }

    pub(in crate::link::egress::load::credit) fn refresh(
        &mut self,
        lane: credit::Lane<'_, 'p, 'd, credit::Ready<'p, 'd>>,
    ) {
        let index = tree::Index::from(lane);
        let (mut records, mut nodes) = self.store.parts();
        let State::Waiting(record) = Records::state(&mut records, index) else {
            return;
        };
        let waiting = Queued::value(&record);
        let demand = credit::Demand::queued(lane.account(), waiting.amount);
        let proof = record.proof();
        self.tree.refresh(&mut nodes, proof, demand);
    }

    pub(in crate::link::egress::load::credit) fn select<'pool>(
        &mut self,
        scope: &'pool credit::Pool<'p, 'd, credit::Ready<'p, 'd>>,
        shared: credit::Amount,
    ) -> Selection<'_, 'pool, 'p, 'd> {
        let nodes = self.store.nodes();
        let Some(oldest) = self.tree.oldest(&nodes) else {
            return Selection::NoWaiting;
        };
        let Some(selected) = self.tree.eligible(&nodes, shared) else {
            return Selection::Insufficient;
        };
        let bypass: Option<Bypassable> = if selected != oldest {
            let limit = nodes.len() - 1;
            match self.store.bypass(oldest, limit) {
                Bypass::Admitted(proof) => Some(proof),
                Bypass::Exhausted => return Selection::BypassExhausted,
            }
        } else {
            None
        };
        let (mut records, mut nodes) = self.store.parts();
        if let Some(proof) = bypass {
            Records::admit(&mut records, proof);
        }
        let (selected, proof) = Records::selected(records, selected, scope);
        self.tree.remove(&mut nodes, proof);
        Selection::Selected(selected)
    }

    pub(in crate::link::egress::load::credit) fn resume(
        &mut self,
        lane: credit::Lane<'_, 'p, 'd, credit::Ready<'p, 'd>>,
        key: ready::Retained<'d>,
        amount: credit::Queued,
    ) -> Result<Resume<'p>, egress::Tickets> {
        let index = tree::Index::from(lane);
        let (mut records, mut nodes) = self.store.parts();
        let record = match Records::state(&mut records, index) {
            State::Idle(record) => record,
            State::Waiting(record) => {
                let waiting = Queued::value(&record);
                if waiting.key == key && waiting.amount == amount {
                    return Ok(Resume::Waiting);
                }
                let demand = credit::Demand::queued(lane.account(), amount);
                let change = record.replace(Waiting {
                    key,
                    amount,
                    bypasses: waiting.bypasses,
                });
                self.tree.refresh(&mut nodes, change, demand);
                return Ok(Resume::Queued);
            }
            State::Granted(record) if Ready::matches(&record, key, amount) => {
                return Ok(Resume::Granted(Ready::take(record)));
            }
            State::Granted(record) => {
                let (idle, grant) = Ready::vacate(record);
                grant.release();
                idle
            }
        };
        Vacant {
            record,
            tree: &mut self.tree,
            nodes: &mut nodes,
            ticket: &mut self.ticket,
            index,
        }
        .poll(lane, key, amount)
    }

    pub(in crate::link::egress::load::credit) fn cancel(
        &mut self,
        lane: credit::Lane<'_, 'p, 'd, credit::Ready<'p, 'd>>,
    ) -> Cancelled {
        let index = tree::Index::from(lane);
        let (mut records, mut nodes) = self.store.parts();
        match Records::state(&mut records, index) {
            State::Idle(_) => Cancelled::Idle,
            State::Waiting(record) => {
                let proof = Queued::clear(record);
                self.tree.remove(&mut nodes, proof);
                Cancelled::Busy
            }
            State::Granted(record) => {
                Ready::take(record).release();
                Cancelled::Busy
            }
        }
    }
}

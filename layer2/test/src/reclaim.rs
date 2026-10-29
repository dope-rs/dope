use dope_core::{
    driver::{
        result::poll::Close,
        route::{self, indices::SlotIndex, kind::Any},
        settings,
    },
    io::{Reclaimed, fd::handles},
};

pub trait Matches<E> {
    fn matches(&self, event: &E) -> bool;
}

#[derive(Debug)]
pub enum Error {
    AcceptedIndex { event: Reclaimed, capacity: usize },
    AcceptedOwner(Reclaimed),
    ClosedIndex { event: Reclaimed, capacity: usize },
    ClosedOwner(Reclaimed),
    Close(Reclaimed),
}

#[derive(Debug)]
pub struct Reports<const ROUTES: usize> {
    accepted: usize,
    capacity: usize,
    routes: [route::Route; ROUTES],
    closed: Vec<handles::PhysicalSlot>,
    events: Vec<Reclaimed>,
    failure: Option<Error>,
}

impl<const ROUTES: usize> Reports<ROUTES> {
    pub fn new<R: Into<route::Route>>(config: settings::Config, routes: [R; ROUTES]) -> Self {
        let files = config.file_slots();
        Self {
            accepted: files.accept().count().get(),
            capacity: files.capacity().count().get(),
            routes: routes.map(Into::into),
            closed: Vec::new(),
            events: Vec::new(),
            failure: None,
        }
    }

    pub fn push(&mut self, event: Reclaimed) {
        if let Err(error) = self.try_push(event) {
            self.failure.get_or_insert(error);
        }
    }

    fn try_push(&mut self, event: Reclaimed) -> Result<(), Error> {
        let origin = <route::FrameworkTag<Any> as route::Tag>::ROUTE;
        if let Reclaimed::Accept {
            result: Ok(handles::Reclaimed::Queued(slot) | handles::Reclaimed::Quarantined(slot)),
            ..
        } = &event
        {
            if (SlotIndex::from(*slot).index().get() as usize) >= self.accepted {
                return Err(Error::AcceptedIndex {
                    event,
                    capacity: self.accepted,
                });
            }
            if slot.route() != origin {
                return Err(Error::AcceptedOwner(event));
            }
        }
        match event {
            Reclaimed::Close {
                slot,
                file: Close::Closed(Ok(())),
                reactor: Ok(()),
            } => {
                let index = SlotIndex::from(slot).index().get() as usize;
                if index >= self.capacity {
                    return Err(Error::ClosedIndex {
                        event,
                        capacity: self.capacity,
                    });
                }
                let admitted = if index < self.accepted {
                    slot.route() == origin
                } else {
                    self.routes.contains(&slot.route())
                };
                if !admitted {
                    return Err(Error::ClosedOwner(event));
                }
                self.closed.push(slot);
            }
            event @ Reclaimed::Close { .. } => return Err(Error::Close(event)),
            event => self.events.push(event),
        }
        Ok(())
    }

    pub fn closed(&self) -> Result<&[handles::PhysicalSlot], &Error> {
        self.try_observed()?;
        Ok(&self.closed)
    }
    pub fn events(&self) -> Result<&[Reclaimed], &Error> {
        self.try_observed()?;
        Ok(&self.events)
    }
    pub fn into_events(self) -> Result<Vec<Reclaimed>, Error> {
        self.failure.map_or(Ok(self.events), Err)
    }
    pub fn matches_one(&self, expected: &impl Matches<Reclaimed>) -> Result<bool, &Error> {
        Ok(matches!(self.events()?, [event] if expected.matches(event)))
    }

    fn try_observed(&self) -> Result<(), &Error> {
        self.failure.as_ref().map_or(Ok(()), Err)
    }
}

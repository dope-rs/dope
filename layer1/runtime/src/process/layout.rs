use std::{convert, mem, num::NonZeroUsize, vec};

use dope_core::platform::affinity;
use o3::alloc::{self, VecExt as _};

use crate::process::{
    self,
    failure::{config, create, input},
};

/// Validated, ordered CPU topology used to map one exact startup
/// resource per worker.
pub struct Layout {
    head: affinity::Cpu,
    tail: Vec<affinity::Cpu>,
}

pub(in crate::process) struct Assignment<I> {
    pub(in crate::process) cpu: affinity::Cpu,
    pub(in crate::process) input: I,
}

pub(in crate::process) struct Set<I> {
    prior: Vec<Assignment<I>>,
    last: Assignment<I>,
}

impl Layout {
    pub fn pinned(cpus: impl IntoIterator<Item = affinity::Cpu>) -> Result<Self, create::Error> {
        let mut source = cpus.into_iter();
        let Some(head) = source.next() else {
            return Err(create::Error::Config(config::Error::Empty));
        };
        let mut tail = Vec::new();
        alloc::VecExt::try_reserve(&mut tail, source.size_hint().0)?;
        for cpu in source {
            if cpu == head || tail.contains(&cpu) {
                return Err(create::Error::Config(config::Error::DuplicateCpu(cpu)));
            }
            alloc::Vacant::try_new(&mut tail)?.insert(cpu);
        }
        Ok(Self { head, tail })
    }

    pub fn count(&self) -> NonZeroUsize {
        NonZeroUsize::MIN.saturating_add(self.tail.len())
    }

    /// Assigns exactly one affine input to each CPU, in topology order.
    pub fn try_assign<I, R>(self, items: R) -> Result<process::Runtime<I>, create::Error>
    where
        R: IntoIterator<Item = I>,
        R::IntoIter: ExactSizeIterator,
    {
        self.try_map(items, convert::identity)
    }

    /// Consumes exactly one affine resource for each CPU, in topology order.
    pub fn try_map<T, I, R>(
        self,
        items: R,
        issue: impl Fn(T) -> I,
    ) -> Result<process::Runtime<I>, create::Error>
    where
        R: IntoIterator<Item = T>,
        R::IntoIter: ExactSizeIterator,
    {
        let mut items = Input::new(items.into_iter(), self.count())?;
        let mut prior = Vec::try_new(self.tail.len())?;
        let item = items.next(0)?;
        let mut last = Assignment {
            cpu: self.head,
            input: issue(item),
        };
        for cpu in self.tail {
            let item = items.next(prior.len() + 1)?;
            let previous = mem::replace(
                &mut last,
                Assignment {
                    cpu,
                    input: issue(item),
                },
            );
            prior.push(previous);
        }
        items.finish()?;
        Ok(process::Runtime {
            workers: Set { prior, last },
        })
    }

    /// Replicates prior inputs and moves the original owner into the final CPU.
    pub fn try_fork<O, T, I, E, R>(
        self,
        owner: O,
        items: R,
        issue: impl Fn(&O, T) -> Result<I, E>,
        finish: impl FnOnce(O, T) -> I,
    ) -> Result<process::Runtime<I>, input::Error<E>>
    where
        R: IntoIterator<Item = T>,
        R::IntoIter: ExactSizeIterator,
    {
        let mut items =
            Input::new(items.into_iter(), self.count()).map_err(input::Error::Layout)?;
        let mut prior = Vec::try_new(self.tail.len())
            .map_err(create::Error::Allocation)
            .map_err(input::Error::Layout)?;
        let item = items.next(0).map_err(input::Error::Layout)?;
        let mut last = Assignment {
            cpu: self.head,
            input: item,
        };
        for cpu in self.tail {
            let item = items.next(prior.len() + 1).map_err(input::Error::Layout)?;
            let input = issue(&owner, last.input).map_err(|cause| input::Error::Replica {
                cpu: last.cpu,
                cause,
            })?;
            prior.push(Assignment {
                cpu: last.cpu,
                input,
            });
            last = Assignment { cpu, input: item };
        }
        items.finish().map_err(input::Error::Layout)?;
        let last = Assignment {
            cpu: last.cpu,
            input: finish(owner, last.input),
        };
        Ok(process::Runtime {
            workers: Set { prior, last },
        })
    }
}

struct Input<I> {
    source: I,
    required: NonZeroUsize,
}

impl<I: ExactSizeIterator> Input<I> {
    fn new(source: I, required: NonZeroUsize) -> Result<Self, create::Error> {
        let actual = source.len();
        if actual != required.get() {
            return Err(create::Error::Config(config::Error::ResourceCount {
                actual,
                required: required.get(),
            }));
        }
        Ok(Self { source, required })
    }

    fn next(&mut self, yielded: usize) -> Result<I::Item, create::Error> {
        self.source
            .next()
            .ok_or(create::Error::Config(config::Error::ResourceIterator {
                yielded,
                required: self.required.get(),
            }))
    }

    fn finish(&mut self) -> Result<(), create::Error> {
        if self.source.next().is_some() {
            return Err(create::Error::Config(config::Error::ResourceIterator {
                yielded: self.required.get() + 1,
                required: self.required.get(),
            }));
        }
        Ok(())
    }
}

impl<I> Set<I> {
    pub(in crate::process) fn len(&self) -> usize {
        self.prior.len() + 1
    }

    pub(in crate::process) fn into_parts(self) -> (vec::IntoIter<Assignment<I>>, Assignment<I>) {
        (self.prior.into_iter(), self.last)
    }
}

const _: () = {
    assert!(mem::size_of::<Layout>() == 4 * mem::size_of::<usize>());
    assert!(mem::size_of::<Set<()>>() == 4 * mem::size_of::<usize>());
};

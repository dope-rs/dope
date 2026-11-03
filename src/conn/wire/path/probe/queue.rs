use crate::conn::output::control::{self, owner};
use crate::conn::wire::storage;
use crate::wire::varint;
use o3::depot::{array::copy, capacity};

use crate::conn;
use crate::conn::output::control::journal::storage::Set;
use crate::conn::wire::path;
use crate::conn::wire::path::probe::{self, Completion};

pub(in crate::conn) struct Queue<'a> {
    state: &'a mut probe::State,
    control: &'a mut Set,
}

pub(in crate::conn) struct Challenges<'a> {
    values: &'a mut storage::Split<probe::Challenge, { conn::MAX_PATH_TOKENS }>,
    control: &'a mut Set,
}

impl<'a> Queue<'a> {
    pub(in crate::conn) fn cancel(&mut self, data: [u8; 8]) -> bool {
        self.state
            .challenges
            .remove_all(
                |probe| probe.path.is_none() && probe.data == data,
                |mut probe| self.control.remove(&mut probe.control),
            )
            .is_some()
    }

    pub(in crate::conn) fn try_respond(
        &mut self,
        token: [u8; 8],
    ) -> Result<Completion, capacity::Reason> {
        let Some(entry) = self
            .state
            .challenges
            .select(|probe| probe.data == token && !matches!(probe.sent, probe::Sent::Pending))
        else {
            return Ok(Completion::Unmatched);
        };
        if !self.state.validated_tokens.contains(&entry.get().data) {
            self.state
                .validated_tokens
                .try_reuse()?
                .push(entry.get().data);
        }
        let mut probe = entry.take();
        self.control.remove(&mut probe.control);
        Ok(Completion::Accepted {
            path: probe.path,
            expanded: matches!(probe.sent, probe::Sent::Expanded),
        })
    }

    /// Keeps an inclusive no-reuse cutoff, including after retirement ACK.
    /// `entries` contains distinct known IDs and marks those kept by commit.
    pub(in crate::conn) fn try_retire(
        &mut self,
        through: varint::VarInt,
        entries: impl Iterator<Item = (varint::VarInt, bool)>,
    ) -> Result<(), capacity::Reason> {
        let end = self.state.retired.max(through.get());
        let mut retiring =
            copy::Vec::<varint::VarInt, { conn::MAX_PENDING_RETIRE_CONNECTION_IDS }>::new();
        for (sequence, keep) in entries {
            if keep
                || self
                    .state
                    .retirements
                    .iter()
                    .any(|entry| entry.0 == sequence)
            {
                continue;
            }
            retiring
                .vacancy()
                .ok_or(capacity::Reason::Full)?
                .insert(sequence);
        }
        if retiring.as_slice().is_empty() {
            self.state.retired = end;
            return Ok(());
        }
        self.state
            .retirements
            .try_extend(retiring.as_slice())?
            .commit(|&sequence| (sequence, None));
        self.state.retired = end;
        Ok(())
    }

    pub(in crate::conn) fn reset(&mut self) {
        self.state.flags.reset();
    }

    pub(in crate::conn) fn retry(&mut self) {
        self.state.flags.retry();
    }

    pub(in crate::conn) fn reconcile(&mut self, mut work: usize) -> Result<(), control::Admission> {
        work = work.min(self.control.available().get());
        while work != 0 {
            let Some(mut pending) = self.state.response_controls.first() else {
                break;
            };
            let reply = pending.get_mut();
            let Some(vacant) = owner::Vacant::new(&mut reply.control) else {
                pending.commit();
                continue;
            };
            vacant.try_queue(self.control, reply.key)?;
            pending.commit();
            work -= 1;
        }
        while work != 0 {
            let Some(mut pending) = self.state.retirements.first() else {
                break;
            };
            let (sequence, owner) = pending.get_mut();
            let Some(vacant) = owner::Vacant::new(owner) else {
                pending.commit();
                continue;
            };
            vacant.try_queue(self.control, *sequence)?;
            pending.commit();
            work -= 1;
        }
        while work != 0 {
            let Some(mut pending) = self.state.challenges.first() else {
                break;
            };
            let probe = pending.get_mut();
            let Some(vacant) = owner::Vacant::new(&mut probe.control) else {
                pending.commit();
                continue;
            };
            vacant.try_queue(self.control, probe.key, probe.path)?;
            pending.commit();
            work -= 1;
        }
        Ok(())
    }

    pub(super) const fn new(state: &'a mut probe::State, control: &'a mut Set) -> Self {
        Self { state, control }
    }

    pub(in crate::conn) fn try_queue(&mut self, data: [u8; 8]) -> Result<(), probe::Error> {
        self.try_path(data, None)
    }

    pub(in crate::conn) fn try_path(
        &mut self,
        data: [u8; 8],
        path: Option<path::Key>,
    ) -> Result<(), probe::Error> {
        if self.state.challenges.iter().any(|probe| probe.data == data) {
            return Err(probe::Error::Duplicate { data });
        }
        let growth = self
            .state
            .challenges
            .try_insert()
            .map_err(|reason| probe::Error::Capacity { data, reason })?;
        let next = self.state.next_control_key;
        let Some((key, after)) = probe::State::key(next) else {
            return Err(probe::Error::Keys { data, next });
        };
        self.state.next_control_key = after;
        growth.push(probe::Challenge {
            key,
            data,
            control: None,
            sent: probe::Sent::Pending,
            path,
        });
        Ok(())
    }

    pub(in crate::conn) fn try_pair(
        &mut self,
        data: [[u8; 8]; 2],
        paths: [path::Key; 2],
    ) -> Result<(), probe::Error> {
        for (index, value) in data.iter().enumerate() {
            if data[..index].contains(value)
                || self
                    .state
                    .challenges
                    .iter()
                    .any(|probe| probe.data == *value)
            {
                return Err(probe::Error::Duplicate { data: *value });
            }
        }
        let next = self.state.next_control_key;
        let Some((first, next)) = probe::State::key(next) else {
            return Err(probe::Error::Keys {
                data: data[0],
                next,
            });
        };
        let Some((second, after)) = probe::State::key(next) else {
            return Err(probe::Error::Keys {
                data: data[1],
                next,
            });
        };
        let entries = [(first, data[0], paths[0]), (second, data[1], paths[1])];
        let growth = self
            .state
            .challenges
            .try_extend(&entries)
            .map_err(|reason| probe::Error::Capacity {
                data: data[0],
                reason,
            })?;
        self.state.next_control_key = after;
        growth.commit(|&(key, data, path)| probe::Challenge {
            key,
            data,
            control: None,
            sent: probe::Sent::Pending,
            path: Some(path),
        });
        Ok(())
    }
}

impl<'a> Challenges<'a> {
    pub(super) const fn new(
        values: &'a mut storage::Split<probe::Challenge, { conn::MAX_PATH_TOKENS }>,
        control: &'a mut Set,
    ) -> Self {
        Self { values, control }
    }

    pub(in crate::conn) fn abandon(&mut self, path: path::Key) {
        let _ = self.values.remove_all(
            |probe| probe.path == Some(path),
            |mut probe| self.control.remove(&mut probe.control),
        );
    }
}

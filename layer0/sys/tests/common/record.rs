use std::num;

use dope_sys::Errno;

#[derive(Debug)]
pub struct Transfer<'a> {
    input: &'a [u8],
    steps: &'a [Result<usize, Errno>],
    pub sent: Vec<u8>,
}

impl<'a> Transfer<'a> {
    pub fn new(input: &'a [u8], steps: &'a [Result<usize, Errno>]) -> Self {
        Self {
            input,
            steps,
            sent: Vec::new(),
        }
    }

    fn step(&mut self) -> Result<usize, Errno> {
        match self.steps.split_first() {
            Some((step, rest)) => {
                self.steps = rest;
                *step
            }
            None => Ok(usize::MAX),
        }
    }

    pub fn send<'b>(&mut self, input: &'b [u8]) -> Result<&'b [u8], Errno> {
        let extent = self.step()?.min(input.len());
        let sent = &input[..extent];
        self.sent.extend_from_slice(sent);
        Ok(sent)
    }

    pub fn recv<'b>(&mut self, output: &'b mut [u8]) -> Result<&'b mut [u8], Errno> {
        let extent = self.step()?.min(output.len()).min(self.input.len());
        let (received, rest) = self.input.split_at(extent);
        self.input = rest;
        let output = &mut output[..extent];
        output.copy_from_slice(received);
        Ok(output)
    }
}

pub fn frame(bytes: &[u8]) -> Result<Vec<u8>, num::TryFromIntError> {
    let length = u8::try_from(bytes.len())?;
    let mut frame = Vec::with_capacity(bytes.len() + 1);
    frame.push(length);
    frame.extend_from_slice(bytes);
    Ok(frame)
}

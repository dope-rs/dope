use std::time::Instant;

use dope_runtime::civil;
use o3::base::time::{Nanos, Unix};

pub struct Source(pub Unix<Nanos>, pub Instant);

impl civil::Source for Source {
    fn read(&self) -> Result<Unix<Nanos>, civil::Error> {
        Ok(self.0)
    }

    fn sample(&self) -> Result<(Unix<Nanos>, Instant), civil::Error> {
        Ok((self.0, self.1))
    }
}

use dope::net::{
    tcp::Tcp,
    wire::{Identity, open},
};
use dope_core::driver::route::KeyTag;
use dope_fiber::net::{read::Lease, server::Listener};
use o3::buffer::{
    Source as _,
    write::consume::{self, Consume as _},
};

pub const MAX_CONN: usize = 8;

pub trait Append {
    fn try_append(&mut self, output: &mut Vec<u8>, limit: usize) -> Result<(), consume::Error>;
}

impl Append for Lease<'_, '_, '_, Identity> {
    fn try_append(&mut self, output: &mut Vec<u8>, limit: usize) -> Result<(), consume::Error> {
        while output.len() < limit && !self.is_empty() {
            let chunk = self.as_ref();
            let amount = chunk.len().min(limit - output.len());
            output.extend_from_slice(&chunk[..amount]);
            self.try_consume(amount)?;
        }
        Ok(())
    }
}

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
pub struct App<'n, 'p, 's, 'scope, 'w, 'd: 's + 'w, F: FnMut(open::Error<'w, Identity>)> {
    #[pin]
    #[manifold]
    pub(crate) pool: Listener<'n, 'p, 's, 'scope, 'w, 'd, KeyTag<0>, Tcp, Identity, F>,
}

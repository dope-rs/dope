use dope::net::wire::Identity;
use dope_fiber::net::read::Lease;
use o3::buffer::{
    Source as _,
    write::consume::{self, Consume as _},
};

pub const MAX_CONN: usize = 8;

pub fn try_append(
    lease: &mut Lease<'_, '_, '_, Identity>,
    output: &mut Vec<u8>,
    limit: usize,
) -> Result<(), consume::Error> {
    while output.len() < limit && !lease.is_empty() {
        let chunk = lease.as_ref();
        let amount = chunk.len().min(limit - output.len());
        output.extend_from_slice(&chunk[..amount]);
        lease.try_consume(amount)?;
    }
    Ok(())
}

use dope_fiber::net::read::Lease;
use o3::buffer::{
    hold::{self, scope::Input},
    resident,
    write::consume::Consume as _,
};

pub fn retain<'d>(
    lease: &mut Lease<'_, '_, 'd, super::Wire>,
    budget: &resident::Budget<'d>,
) -> Result<resident::Shared<'d>, hold::Error> {
    let input = Input::new(&**lease, budget);
    let bytes = input.as_slice();
    assert!(!bytes.is_empty());
    let saved = input.defer_all().bind(hold::Policy::share()).try_into()?;
    assert_eq!(saved.as_slice(), bytes);
    assert_eq!(saved.as_slice().as_ptr(), bytes.as_ptr());
    let count = bytes.len();
    lease.try_consume(count).expect("consume retained plaintext");
    Ok(saved)
}

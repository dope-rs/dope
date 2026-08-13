use dope_manifold::service::attach::{AlreadyAttached, Attach, Bound};

#[test]
fn bind_is_irreversible_after_bound_drop() {
    let attach = Attach::new();
    let value = 7;
    {
        let _bound = attach.bind(&value).expect("first bind");
    }
    assert!(matches!(attach.bind(&value), Err(AlreadyAttached)));
}

#[test]
fn bind_is_irreversible_after_later_construction_failure() {
    fn construct<'d>(attach: &Attach, value: &'d u64) -> Result<Bound<'d, u64>, &'static str> {
        let _bound = attach.bind(value).map_err(|_| "already attached")?;
        Err("construction failed")
    }

    let attach = Attach::new();
    let value = 7;
    assert!(construct(&attach, &value).is_err());
    assert!(matches!(attach.bind(&value), Err(AlreadyAttached)));
}

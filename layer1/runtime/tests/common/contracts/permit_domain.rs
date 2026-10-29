use dope_runtime::topology::lending;

fn substitute<'issued: 'other, 'other, 'life: 'issued, 'd: 'life, O, Q>(
    permit: lending::Permit<'issued, 'life, 'd, O, Q>,
) -> lending::Permit<'other, 'life, 'd, O, Q> {
    permit
}

fn main() {}

use dope_fiber::local::{LocalCell, LocalContext};

fn escape<'d>(cell: &LocalCell<'d, String>, local: &LocalContext<'_, 'd>) -> &'d str {
    cell.read_with(local, |value| value.as_str())
}

fn main() {}

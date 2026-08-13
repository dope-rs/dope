Turn-scoped maintenance budget divided among a compile-time participant count.

The count cannot be zero:

```compile_fail,E0080
use dope_core::driver::schedule;

fn zero(work: schedule::Maintenance<'static, 'static>) {
    drop(work.share::<0>());
}

fn main() {
    std::hint::black_box(zero as fn(schedule::Maintenance<'static, 'static>));
}
```

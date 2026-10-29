mod ring;

use dope_sys::table::permit;
use dope_uring::Uring;
use ring::key::Lifetime;

type Queue<'a> = <Uring as permit::Source>::Queue<Lifetime<'a>, u32>;

fn widen<'a, 'b: 'a>(fifo: Queue<'a>) -> Queue<'b> {
    fifo
}

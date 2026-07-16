extern crate dope;

use o3::cell::{BrandCell, BrandToken};

fn main() {
    BrandToken::scope(|mut first| {
        BrandToken::scope(|mut second| {
            let cell = BrandCell::new(0);
            let _ = cell.borrow_mut(&mut first);
            let _ = cell.borrow_mut(&mut second);
        });
    });
}

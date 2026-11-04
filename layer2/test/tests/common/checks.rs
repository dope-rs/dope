use std::{os::unix::process::ExitStatusExt as _, process};

pub const PACKAGES: &[&str] = &["alias=proof"];
pub const PATH: &str = "alias=/fresh/a \"quoted\" 🚀/libproof-123.rlib";
pub const NATIVE: &str = "native=/fresh/with space/lib";

pub const ARTIFACT: &str = r#"{"unused":{"reason":"decoy","array":[null,true,-1.25e+3,"\"\\\/\b\f\n\r\t\u0000\ud83d\ude80"]},"target":{"name":"proof","kind":["lib"]},"filenames":["/fresh/a \"quoted\" \ud83d\ude80/libproof-123.rlib"],"package_id":"path+file:///fresh#proof@1.0.0","profile":{"test":false},"rea\u0073on":"compiler-artifact"}"#;
pub const NATIVE_PATH: &str =
    r#"{"reason":"build-script-executed","linked_paths":["native=/fresh/with space/lib"]}"#;
pub const FOREIGN: &str = r#"{"reason":"compiler-artifact","target":{"name":"proof","kind":["lib"]},"filenames":["/other/libproof-456.rlib"],"package_id":"registry+remote#proof@1.0.0","profile":{"test":false}}"#;

pub const INVALID: &[(&str, usize)] = &[
    (r#"{"reason":"build-finished",}"#, 27),
    (r#"{"reason":"build-finished","unused":01}"#, 36),
    (r#"{"reason":"build-finished","unused":"\ud800"}"#, 37),
    (r#"{"reason":"build-finished","unused":"\udc00"}"#, 37),
    (r#"{"reason":"build-finished","unused":"\uXXXX"}"#, 37),
    (r#"{"reason":"build-finished","unused":[1,]}"#, 39),
    ("{\"reason\":\"build-finished\",\"unused\":\"a\tb\"}", 37),
    (r#"{"reason":"build-finished","unused":"\q"}"#, 37),
    (r#"{"reason":"build-finished","unused":"\ud800\u0041"}"#, 37),
];

pub const MIXED_ERRORS: &str =
    "//! ```compile_fail,E0308\n//! let _: u8 = \"wrong type\";\n//! missing_function();\n//! ```";

pub fn output(stdout: &str) -> process::Output {
    process::Output {
        status: process::ExitStatus::from_raw(0),
        stdout: stdout.as_bytes().to_vec(),
        stderr: Vec::new(),
    }
}

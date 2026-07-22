cfg_select! {
    target_os = "linux" => {
        mod raw;
        pub(super) use raw::SignalState;
    }
    _ => {
        mod unsupported;
        pub(super) use unsupported::SignalState;
    }
}

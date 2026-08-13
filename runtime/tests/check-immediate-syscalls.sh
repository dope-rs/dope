#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
WORKSPACE=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
cd "$WORKSPACE"

if ! command -v strace >/dev/null 2>&1; then
    echo "error: strace is required to verify the immediate-drive syscall contract" >&2
    exit 1
fi

TRACE_DIR=$(mktemp -d)
trap 'rm -rf "$TRACE_DIR"' EXIT

cargo test -q -p dope-runtime --release --test executor --no-run

BASELINE_TEST=client::immediate_drive_syscall_baseline
PROBE_TEST=client::immediate_drive_syscall_probe_allocates_nothing

require_test() {
    test_name=$1

    if ! cargo test -q -p dope-runtime --release --test executor \
        "$test_name" -- --exact --list \
        | awk -v expected="$test_name: test" '$0 == expected { found = 1 } END { exit !found }'
    then
        echo "error: executor test not found: $test_name" >&2
        exit 1
    fi
}

trace_test() {
    test_name=$1
    trace_file=$2

    strace -f -qq -e trace=io_uring_enter -o "$trace_file" \
        cargo test -q -p dope-runtime --release --test executor \
        "$test_name" -- --exact
}

count_enters() {
    awk '/io_uring_enter/ { count += 1 } END { print count + 0 }' "$1"
}

BASELINE_TRACE="$TRACE_DIR/baseline.trace"
PROBE_TRACE="$TRACE_DIR/probe.trace"

require_test "$BASELINE_TEST"
require_test "$PROBE_TEST"
trace_test "$BASELINE_TEST" "$BASELINE_TRACE"
trace_test "$PROBE_TEST" "$PROBE_TRACE"

baseline_enters=$(count_enters "$BASELINE_TRACE")
probe_enters=$(count_enters "$PROBE_TRACE")

if [ "$probe_enters" -ne "$baseline_enters" ]; then
    echo "immediate-drive syscall contract violated:" >&2
    echo "  baseline io_uring_enter calls: $baseline_enters" >&2
    echo "  probe io_uring_enter calls:    $probe_enters" >&2
    exit 1
fi

echo "immediate-drive syscall contract holds ($probe_enters setup/teardown io_uring_enter calls)"

// Parity tests for the two rsync helper utilities surfaced by hustsync-internal::util.
//
// These tests prove that the Rust implementations match the Go originals in
// tunasync/internal/util.go:
//   - ExtractSizeFromRsyncLog  → extract_size_from_rsync_log
//   - TranslateRsyncErrorCode  → translate_rsync_exit_status
//
// Each assertion is derived from the Go source behaviour:
//   * ExtractSizeFromLog returns the first capture group of the *last* match.
//   * TranslateRsyncErrorCode formats "rsync error: <msg>" for known codes;
//     returns an empty string for unknown codes.
//
// Fixtures live under tests/fixtures/rsync-logs/.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;

fn fixture_path(name: &str) -> String {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/fixtures/rsync-logs");
    p.push(name);
    p.to_str().unwrap().to_owned()
}

// ---------------------------------------------------------------------------
// extract_size_from_rsync_log — §3.3 of docs/rust-port/05-provider-contract.md
// ---------------------------------------------------------------------------

#[test]
fn extract_size_simple_returns_bare_number() {
    // success-simple.log contains "Total file size: 1234 bytes"
    // The regex captures the numeric portion without " bytes"; no SI suffix.
    let result =
        hustsync_internal::util::extract_size_from_rsync_log(&fixture_path("success-simple.log"))
            .expect("should not error on valid log");
    assert_eq!(result, "1234");
}

#[test]
fn extract_size_human_returns_verbatim_suffix() {
    // success-human.log contains "Total file size: 234.56G bytes"
    // The regex captures "234.56G" — the suffix letter is included verbatim.
    let result =
        hustsync_internal::util::extract_size_from_rsync_log(&fixture_path("success-human.log"))
            .expect("should not error on valid log");
    assert_eq!(result, "234.56G");
}

#[test]
fn extract_size_multi_total_returns_last_match() {
    // success-multi-total.log has two "Total file size:" lines:
    //   first  → 100 bytes
    //   second → 55.2M bytes
    // Last match must win, per Go's FindAllSubmatch-last semantics.
    let result = hustsync_internal::util::extract_size_from_rsync_log(&fixture_path(
        "success-multi-total.log",
    ))
    .expect("should not error on valid log");
    assert_eq!(result, "55.2M");
}

#[test]
fn extract_size_error_log_returns_empty() {
    // error-23.log has no "Total file size:" line — partial transfer error.
    // Go returns ""; Rust must also return an empty string (not an error).
    let result =
        hustsync_internal::util::extract_size_from_rsync_log(&fixture_path("error-23.log"))
            .expect("should not error on error log");
    assert_eq!(result, "");
}

#[test]
fn extract_size_empty_log_returns_empty() {
    // empty.log is a zero-byte file.
    let result = hustsync_internal::util::extract_size_from_rsync_log(&fixture_path("empty.log"))
        .expect("should not error on empty log");
    assert_eq!(result, "");
}

// ---------------------------------------------------------------------------
// translate_rsync_exit_status — §3.4 exit-code table
// ---------------------------------------------------------------------------
//
// The Go map in tunasync/internal/util.go lists exactly these codes.
// For each code the Rust function must return a non-empty stable string that
// begins with "rsync error: " (mirroring Go's fmt.Sprintf("rsync error: %s")).
//
// We produce real ExitStatus values by running `sh -c "exit N"` and collecting
// the status — no unsafe casting needed.

fn exit_status_for(code: i32) -> std::process::ExitStatus {
    std::process::Command::new("sh")
        .arg("-c")
        .arg(format!("exit {}", code))
        .status()
        .expect("sh must be available")
}

fn assert_known_code(code: i32, expected_suffix: &str) {
    let status = exit_status_for(code);
    let (returned_code, msg) = hustsync_internal::util::translate_rsync_exit_status(&status);
    assert_eq!(returned_code, Some(code), "code mismatch for exit {code}");
    let msg = msg.unwrap_or_else(|| panic!("expected Some(msg) for known code {code}"));
    assert!(
        !msg.is_empty(),
        "message must be non-empty for known code {code}"
    );
    assert!(
        msg.starts_with("rsync error: "),
        "message must start with 'rsync error: ' for code {code}, got: {msg:?}"
    );
    assert!(
        msg.contains(expected_suffix),
        "message for code {code} should contain {expected_suffix:?}, got: {msg:?}"
    );
}

#[test]
fn translate_known_exit_codes_are_non_empty_stable_strings() {
    // The 19 known exit codes from the Go rsyncExitValues map.
    // Each tuple is (exit_code, substring_from_go_message).
    let known: &[(i32, &str)] = &[
        (0, "Success"),
        (1, "Syntax or usage error"),
        (2, "Protocol incompatibility"),
        (3, "Errors selecting input/output files, dirs"),
        (4, "Requested action not supported"),
        (5, "Error starting client-server protocol"),
        (6, "Daemon unable to append to log-file"),
        (10, "Error in socket I/O"),
        (11, "Error in file I/O"),
        (12, "Error in rsync protocol data stream"),
        (13, "Errors with program diagnostics"),
        (14, "Error in IPC code"),
        (20, "Received SIGUSR1 or SIGINT"),
        (21, "Some error returned by waitpid()"),
        (22, "Error allocating core memory buffers"),
        (23, "Partial transfer due to error"),
        (24, "Partial transfer due to vanished source files"),
        (25, "The --max-delete limit stopped deletions"),
        (30, "Timeout in data send/receive"),
        (35, "Timeout waiting for daemon connection"),
    ];

    for &(code, suffix) in known {
        assert_known_code(code, suffix);
    }
}

#[test]
fn translate_unknown_exit_code_returns_code_but_no_message() {
    // Code 99 is not in the Go map; Go returns exitCode=99, msg="".
    // Rust must return (Some(99), None) — no fabricated message.
    let status = exit_status_for(99);
    let (code, msg) = hustsync_internal::util::translate_rsync_exit_status(&status);
    assert_eq!(code, Some(99));
    assert!(
        msg.is_none(),
        "unknown code 99 must not produce a message, got: {msg:?}"
    );
}

#[test]
fn translate_three_spot_check_exit_codes() {
    // Spot-check a representive trio: 0 (success), 23 (partial), 35 (timeout).
    // These three appear explicitly in the plan §4.1 T2 acceptance criteria.
    let checks: &[(i32, &str)] = &[
        (0, "Success"),
        (23, "Partial transfer due to error"),
        (35, "Timeout waiting for daemon connection"),
    ];
    for &(code, suffix) in checks {
        assert_known_code(code, suffix);
    }
}

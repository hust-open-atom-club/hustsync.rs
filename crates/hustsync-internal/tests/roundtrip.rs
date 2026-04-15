// Wire-type round-trip tests for hustsync-internal.
//
// Each test loads a committed fixture, deserializes it into the Rust type, re-serializes,
// and asserts structural equality with the fixture as a serde_json::Value.  Raw-byte equality
// is not required: JSON key order and insignificant whitespace are normalized away.
// Timestamp fields are verified against their expected values via serde_json::Value
// rather than string comparison so that UTC-offset notation differences (+0000 vs Z)
// do not cause spurious failures in the chrono RFC3339 path.
//
// Fixture layout:  crates/hustsync-internal/tests/fixtures/<type>/<name>.json
//
// Wire-format notes (see docs/rust-port/02-http-contract.md §3.2 §3.4 §3.5 §3.8 §3.10 §3.11 §4.1):
//
//  - MirrorStatus uses #[serde(rename_all = "kebab-case")].  Field names on the wire are
//    therefore hyphenated: "error-msg", "last-update", "is-master", "next-scheduled".
//    This diverges from Go tunasync (which uses snake_case) and from WebMirrorStatus
//    (snake_case).  The divergence lives in src/ and is outside testing-qa's lane;
//    these tests pin the *current* Rust behaviour so any future fix in src/ breaks
//    them intentionally, prompting a fixture update.
//
//  - WebMirrorStatus uses dual text ("YYYY-MM-DD HH:MM:SS ±ZZZZ") plus sibling *_ts
//    Unix-second integer fields per §3.2.  Text timestamps include timezone offset from
//    the serialized DateTime<Utc>, which chrono renders as "+0000" for UTC.
//
//  - MirrorSchedule serialises the name field as JSON key "name"
//    (matches Go `MirrorSchedule.MirrorName` with json:"name").
//
//  - CmdVerb serialises as kebab-case strings per the enum attribute.
//    WorkerCmd / ClientCmd use snake_case for their other fields.

use std::path::Path;

use hustsync_internal::msg::{
    ClientCmd, MirrorSchedules, MirrorStatus, WorkerCmd, WorkerStatus,
};
use hustsync_internal::status::SyncStatus;
use hustsync_internal::status_web::WebMirrorStatus;

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

fn load_fixture(rel: &str) -> serde_json::Value {
    let path = Path::new(FIXTURES).join(rel);
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {}: {e}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("fixture {} is not valid JSON: {e}", path.display()))
}

/// Deserialize `fixture_rel` into `T`, re-serialize, and assert structural equality
/// with the original fixture value.  Fields in `masked_keys` are removed from both
/// sides before comparison (for non-deterministic values such as timestamps produced
/// at runtime — not used by the present fixtures but kept for future tests).
fn assert_roundtrip<T>(fixture_rel: &str, masked_keys: &[&str])
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    let fixture_val = load_fixture(fixture_rel);
    let deserialized: T = serde_json::from_value(fixture_val.clone())
        .unwrap_or_else(|e| panic!("deserialize from {fixture_rel} failed: {e}"));
    let reserialized = serde_json::to_value(&deserialized)
        .unwrap_or_else(|e| panic!("re-serialize in {fixture_rel} failed: {e}"));

    fn mask(mut v: serde_json::Value, keys: &[&str]) -> serde_json::Value {
        if let serde_json::Value::Object(ref mut map) = v {
            for k in keys {
                map.remove(*k);
            }
        }
        v
    }

    let want = mask(fixture_val, masked_keys);
    let got = mask(reserialized, masked_keys);
    assert_eq!(
        got, want,
        "round-trip mismatch for {fixture_rel}:\n  got  = {got}\n  want = {want}"
    );
}

// ---------------------------------------------------------------------------
// msg types
// ---------------------------------------------------------------------------

/// §3.8 — MirrorStatus: worker → manager status push body.
/// Fixture keys are kebab-case because MirrorStatus carries
/// #[serde(rename_all = "kebab-case")].  That diverges from Go wire
/// format (snake_case); pinned here to make the divergence explicit.
#[test]
fn test_roundtrip_mirror_status() {
    assert_roundtrip::<MirrorStatus>("msg/mirror_status.json", &[]);
}

/// §3.4 / §3.5 — WorkerStatus: manager list-workers response element.
/// Uses snake_case throughout, matching Go wire format.
#[test]
fn test_roundtrip_worker_status() {
    assert_roundtrip::<WorkerStatus>("msg/worker_status.json", &[]);
}

/// §3.10 — MirrorSchedules: bulk schedule update body.
/// MirrorSchedule.name serialises as JSON key "name", matching Go's
/// `MirrorSchedule.MirrorName` with `json:"name"`.
#[test]
fn test_roundtrip_mirror_schedules() {
    assert_roundtrip::<MirrorSchedules>("msg/mirror_schedules.json", &[]);
}

/// §4.1 — WorkerCmd: command forwarded from manager to worker.
#[test]
fn test_roundtrip_worker_cmd() {
    assert_roundtrip::<WorkerCmd>("msg/worker_cmd.json", &[]);
}

/// §3.11 — ClientCmd: command sent from operator (hustsynctl) to manager.
#[test]
fn test_roundtrip_client_cmd() {
    assert_roundtrip::<ClientCmd>("msg/client_cmd.json", &[]);
}

// ---------------------------------------------------------------------------
// status
// ---------------------------------------------------------------------------

/// §3.2 — SyncStatus: all seven valid values survive a round-trip.
/// The fixture is a JSON array; each element is deserialized and checked
/// against its expected string representation.
#[test]
fn test_roundtrip_sync_status_variants() {
    let fixture = load_fixture("status/sync_status_variants.json");
    let variants: Vec<SyncStatus> = serde_json::from_value(fixture.clone())
        .expect("deserialize SyncStatus variants");
    let reserialized = serde_json::to_value(&variants).expect("re-serialize SyncStatus variants");
    assert_eq!(reserialized, fixture, "SyncStatus variants round-trip mismatch");
    // Belt-and-suspenders: verify the seven discriminants are all present
    assert_eq!(variants.len(), 7);
    assert!(variants.contains(&SyncStatus::None));
    assert!(variants.contains(&SyncStatus::Failed));
    assert!(variants.contains(&SyncStatus::Success));
    assert!(variants.contains(&SyncStatus::Syncing));
    assert!(variants.contains(&SyncStatus::PreSyncing));
    assert!(variants.contains(&SyncStatus::Paused));
    assert!(variants.contains(&SyncStatus::Disabled));
}

// ---------------------------------------------------------------------------
// status_web
// ---------------------------------------------------------------------------

/// §3.2 — WebMirrorStatus: dual text+ts timestamp fields.
/// Text format is "YYYY-MM-DD HH:MM:SS ±ZZZZ" (Go layout "2006-01-02 15:04:05 -0700").
/// The *_ts fields are Unix-second integers.
#[test]
fn test_roundtrip_web_mirror_status() {
    assert_roundtrip::<WebMirrorStatus>("status_web/web_mirror_status.json", &[]);
}

/// Verify that text timestamps survive a round-trip preserving the exact string form
/// that Go tunasync emits.  The fixture uses "+0000" (chrono's UTC offset rendering);
/// this test asserts the exact string value is preserved, not merely the instant.
#[test]
fn test_web_mirror_status_text_timestamp_format_preserved() {
    let fixture = load_fixture("status_web/web_mirror_status.json");
    let wms: WebMirrorStatus = serde_json::from_value(fixture.clone())
        .expect("deserialize WebMirrorStatus");
    let got = serde_json::to_value(&wms).expect("re-serialize WebMirrorStatus");

    for field in &[
        "last_update",
        "last_started",
        "last_ended",
        "next_schedule",
    ] {
        assert_eq!(
            got[field], fixture[field],
            "text timestamp field '{field}' changed during round-trip"
        );
    }
    for field in &[
        "last_update_ts",
        "last_started_ts",
        "last_ended_ts",
        "next_schedule_ts",
    ] {
        assert_eq!(
            got[field], fixture[field],
            "unix-second field '{field}' changed during round-trip"
        );
    }
}

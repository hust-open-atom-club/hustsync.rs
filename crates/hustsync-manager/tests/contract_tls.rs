//! TLS contract smoke test for hustsync-manager (T9).
//!
//! Proves that the same Router that serves HTTP in the trivial-GET contract
//! tests (T3) also works correctly behind the TLS shell provided by
//! axum-server.  The test deliberately mirrors `get_ping_returns_pong` from
//! contract_get.rs: if the response diverges, the TLS wrapper is introducing
//! a change in behaviour, which is the property being guarded.
//!
//! Certificate generation, trust anchoring, and server teardown all run in
//! memory — no PEM files are written to disk.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod contract;

// ---------------------------------------------------------------------------
// GET /ping over TLS
// ---------------------------------------------------------------------------

/// A TLS manager must return the same `{"message":"pong"}` as the plain-HTTP
/// router.
///
/// The self-signed certificate is generated in-memory by `rcgen`; the
/// `reqwest` client is configured to trust only that certificate, so the test
/// verifies that both the TLS handshake and the application response are
/// correct.
#[tokio::test]
async fn tls_get_ping_matches_http_contract() {
    let (addr, server_handle, cert_der) = contract::spawn_manager_tls().await;

    // Build a reqwest client that trusts the self-signed cert.
    // `add_root_certificate` keeps hostname verification active so the test
    // also exercises that the cert's SAN covers 127.0.0.1.  The self-signed
    // cert is not trusted by any OS trust store, so the connection would fail
    // without this explicit trust anchor.
    let root_cert = reqwest::Certificate::from_der(&cert_der).expect("reqwest: parse DER cert");
    let client = reqwest::Client::builder()
        .add_root_certificate(root_cert)
        .build()
        .expect("reqwest: build TLS client");

    let url = format!("https://127.0.0.1:{}/ping", addr.port());
    let response = client
        .get(&url)
        .send()
        .await
        .expect("GET /ping over TLS failed");

    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "TLS GET /ping must return 200"
    );

    let got: serde_json::Value = response
        .json()
        .await
        .expect("TLS GET /ping response is not valid JSON");

    // Compare against the same fixture used by the plain-HTTP T3 test so that
    // any Router-level divergence introduced by the TLS layer is surfaced.
    let want = contract::load_fixture("ping/response.json");
    contract::assert_json_eq_masked(&got, &want, &[]);

    server_handle.abort();
}

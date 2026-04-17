// Contract tests for the worker `POST /` command endpoint.
//
// Go `worker/worker.go` defines the expected
// response shape for each command path. These tests exercise the handler
// in-process via `tower::ServiceExt::oneshot` so no TCP port is needed.
//
// AppState is constructed by hand:
// - `jobs`: Arc<RwLock<HashMap>> — empty for unknown-mirror / invalid-cmd
// cases; populated with one `MirrorJob` for the happy-path case.
// - `schedule_queue`: Arc<Mutex<ScheduleQueue>> — freshly allocated, empty.
//
// The `MirrorJob` for the happy-path case is built directly from its public
// fields (no provider needed) with a live mpsc channel so `send_ctrl` does
// not block the handler.

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod contract_cmd {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use http_body_util::BodyExt as _;
    use hustsync_internal::msg::{CmdVerb, WorkerCmd};
    use hustsync_worker::MirrorJob;
    use hustsync_worker::schedule::ScheduleQueue;
    use hustsync_worker::server::{AppState, make_http_server};
    use serde_json::Value;
    use tokio::sync::{Mutex, RwLock, mpsc};
    use tower::ServiceExt as _;

    // ── helpers ──────────────────────────────────────────────────────────────

    /// Build a minimal `MirrorJob` with a live channel whose receiver is
    /// returned so the test can drain it and prevent the channel from closing.
    fn make_mirror_job(
        name: &str,
    ) -> (MirrorJob, mpsc::Receiver<hustsync_worker::job::CtrlAction>) {
        let (tx, rx) = mpsc::channel(32);
        let job = MirrorJob {
            name: name.into(),
            tx,
            state: Arc::new(AtomicU32::new(hustsync_worker::job::STATE_NONE)),
            disabled: Arc::new(tokio::sync::Notify::new()),
            interval: tokio::time::Duration::from_secs(3600),
        };
        (job, rx)
    }

    /// Construct an `AppState` with an empty jobs map.
    fn empty_state() -> Arc<AppState> {
        Arc::new(AppState {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            schedule_queue: Arc::new(Mutex::new(ScheduleQueue::new())),
            reload_tx: None,
        })
    }

    /// Construct an `AppState` with one pre-configured `MirrorJob`.
    /// Returns the state and the channel receiver so the test keeps it alive.
    fn state_with_job(
        name: &str,
    ) -> (
        Arc<AppState>,
        mpsc::Receiver<hustsync_worker::job::CtrlAction>,
    ) {
        let (job, rx) = make_mirror_job(name);
        let mut jobs = HashMap::new();
        jobs.insert(name.to_string(), job);
        let state = Arc::new(AppState {
            jobs: Arc::new(RwLock::new(jobs)),
            schedule_queue: Arc::new(Mutex::new(ScheduleQueue::new())),
            reload_tx: None,
        });
        (state, rx)
    }

    /// Send a `POST /` request with the given JSON body and return the status
    /// code plus the parsed JSON body.
    async fn post_cmd(state: Arc<AppState>, body: Value) -> (u16, Value) {
        let router = make_http_server(state);

        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let status = response.status().as_u16();

        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&bytes).unwrap();

        (status, json)
    }

    fn worker_cmd(mirror_id: &str, cmd: CmdVerb) -> Value {
        serde_json::to_value(WorkerCmd {
            options: HashMap::new(),
            args: vec![],
            mirror_id: mirror_id.to_string(),
            cmd,
        })
        .unwrap()
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// Configured mirror + valid verb → 200 {"msg": "OK"}
    #[tokio::test]
    async fn test_valid_cmd_known_mirror_returns_ok() {
        let (state, _rx) = state_with_job("archlinux");

        let (status, body) = post_cmd(state, worker_cmd("archlinux", CmdVerb::Start)).await;

        assert_eq!(status, 200);
        assert_eq!(body["msg"], "OK");
    }

    /// Unknown mirror_id → 404 with Go's verbatim TeX-quote message.
    #[tokio::test]
    async fn test_unknown_mirror_returns_not_found() {
        let state = empty_state();

        let (status, body) = post_cmd(state, worker_cmd("foo", CmdVerb::Start)).await;

        assert_eq!(status, 404);
        assert_eq!(body["msg"], "Mirror ``foo'' not found");
    }

    /// Job-level command with an unrecognised verb (Reload on a mirror) → 406.
    #[tokio::test]
    async fn test_invalid_command_verb_returns_not_acceptable() {
        let (state, _rx) = state_with_job("archlinux");

        // Reload is a worker-level verb (mirror_id must be empty to reach the
        // Reload branch). When mirror_id is non-empty the job-level `_ =>`
        // arm fires, producing 406 "Invalid Command".
        let (status, body) = post_cmd(state, worker_cmd("archlinux", CmdVerb::Reload)).await;

        assert_eq!(status, 406);
        assert_eq!(body["msg"], "Invalid Command");
    }

    /// Worker-level Reload (empty mirror_id) → 200 {"msg": "Reload triggered"}
    #[tokio::test]
    async fn test_reload_worker_level_returns_reload_triggered() {
        let (reload_tx, mut reload_rx) = mpsc::unbounded_channel();
        let state = Arc::new(AppState {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            schedule_queue: Arc::new(Mutex::new(ScheduleQueue::new())),
            reload_tx: Some(reload_tx),
        });

        let (status, body) = post_cmd(state, worker_cmd("", CmdVerb::Reload)).await;

        assert_eq!(status, 200);
        assert_eq!(body["msg"], "Reload triggered");
        assert!(
            reload_rx.recv().await.is_some(),
            "worker-level reload must enqueue a reload request"
        );
    }
}

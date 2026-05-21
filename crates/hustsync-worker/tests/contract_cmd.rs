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

    use chrono::{Duration as ChronoDuration, Utc};
    use http_body_util::BodyExt as _;
    use hustsync_internal::msg::{CmdVerb, WorkerCmd};
    use hustsync_worker::MirrorJob;
    use hustsync_worker::job::{CtrlAction, STATE_DISABLED};
    use hustsync_worker::schedule::ScheduleQueue;
    use hustsync_worker::server::{AppState, make_http_server};
    use serde_json::Value;
    use tokio::sync::{Mutex, RwLock, mpsc};
    use tokio::time::{Duration, timeout};
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

    async fn schedule_job(state: &Arc<AppState>, name: &str) {
        let job = state
            .jobs
            .read()
            .await
            .get(name)
            .expect("job must exist")
            .clone();
        state
            .schedule_queue
            .lock()
            .await
            .add_job(Utc::now() + ChronoDuration::minutes(5), job);
        assert!(
            state
                .schedule_queue
                .lock()
                .await
                .get_jobs()
                .iter()
                .any(|job| job.job_name == name),
            "test setup must schedule {name}"
        );
    }

    async fn assert_schedule_flushed(state: &Arc<AppState>, name: &str) {
        assert!(
            !state
                .schedule_queue
                .lock()
                .await
                .get_jobs()
                .iter()
                .any(|job| job.job_name == name),
            "{name} must be removed from the schedule queue"
        );
    }

    async fn expect_action(rx: &mut mpsc::Receiver<CtrlAction>, expected: CtrlAction) {
        let got = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("timed out waiting for ctrl action")
            .expect("ctrl channel closed before expected action");
        assert_eq!(got, expected);
    }

    async fn expect_no_action(rx: &mut mpsc::Receiver<CtrlAction>) {
        assert!(
            timeout(Duration::from_millis(50), rx.recv()).await.is_err(),
            "command must not send a ctrl action"
        );
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

    /// Go's manager can encode omitted command payloads as JSON null. The Rust
    /// worker must treat those as empty collections instead of rejecting the
    /// request at Axum's JSON extractor boundary.
    #[tokio::test]
    async fn test_valid_cmd_accepts_null_args_and_options() {
        let (state, _rx) = state_with_job("archlinux");
        let body = serde_json::json!({
            "options": null,
            "args": null,
            "mirror_id": "archlinux",
            "cmd": "ping"
        });

        let (status, body) = post_cmd(state, body).await;

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

    /// Stop on an active/non-disabled mirror flushes schedule and sends Stop.
    #[tokio::test]
    async fn test_stop_known_mirror_flushes_schedule_and_sends_stop() {
        let (state, mut rx) = state_with_job("archlinux");
        schedule_job(&state, "archlinux").await;

        let (status, body) =
            post_cmd(Arc::clone(&state), worker_cmd("archlinux", CmdVerb::Stop)).await;

        assert_eq!(status, 200);
        assert_eq!(body["msg"], "OK");
        assert_schedule_flushed(&state, "archlinux").await;
        expect_action(&mut rx, CtrlAction::Stop).await;
    }

    /// Stop on a disabled mirror still returns Go's fixed OK body and does
    /// not send an action because no Go job goroutine would be receiving it.
    #[tokio::test]
    async fn test_stop_disabled_mirror_returns_ok_without_action() {
        let (state, mut rx) = state_with_job("archlinux");
        {
            let jobs = state.jobs.read().await;
            jobs.get("archlinux").unwrap().set_state(STATE_DISABLED);
        }
        schedule_job(&state, "archlinux").await;

        let (status, body) =
            post_cmd(Arc::clone(&state), worker_cmd("archlinux", CmdVerb::Stop)).await;

        assert_eq!(status, 200);
        assert_eq!(body["msg"], "OK");
        assert_schedule_flushed(&state, "archlinux").await;
        expect_no_action(&mut rx).await;
    }

    /// Disable flushes any pending schedule, dispatches Disable to the job,
    /// and waits for the job's disabled notification before returning.
    #[tokio::test]
    async fn test_disable_known_mirror_flushes_schedule_sends_disable_and_waits() {
        let (state, mut rx) = state_with_job("archlinux");
        let disabled = {
            let jobs = state.jobs.read().await;
            Arc::clone(&jobs.get("archlinux").unwrap().disabled)
        };
        schedule_job(&state, "archlinux").await;

        let mut post = tokio::spawn(post_cmd(
            Arc::clone(&state),
            worker_cmd("archlinux", CmdVerb::Disable),
        ));

        let got = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("timed out waiting for Disable action")
            .expect("ctrl channel closed before Disable action");
        assert_eq!(got, CtrlAction::Disable);

        assert!(
            timeout(Duration::from_millis(50), &mut post).await.is_err(),
            "disable response must wait for the disabled notification"
        );

        disabled.notify_waiters();

        let (status, body) = post.await.unwrap();

        assert_eq!(status, 200);
        assert_eq!(body["msg"], "OK");
        assert_schedule_flushed(&state, "archlinux").await;
    }

    /// Disable on an already-disabled mirror is a Go-compatible no-op after
    /// schedule flush: it returns OK and does not send another Disable action.
    #[tokio::test]
    async fn test_disable_disabled_mirror_returns_ok_without_action() {
        let (state, mut rx) = state_with_job("archlinux");
        {
            let jobs = state.jobs.read().await;
            jobs.get("archlinux").unwrap().set_state(STATE_DISABLED);
        }
        schedule_job(&state, "archlinux").await;

        let (status, body) = post_cmd(
            Arc::clone(&state),
            worker_cmd("archlinux", CmdVerb::Disable),
        )
        .await;

        assert_eq!(status, 200);
        assert_eq!(body["msg"], "OK");
        assert_schedule_flushed(&state, "archlinux").await;
        expect_no_action(&mut rx).await;
    }

    /// Restart flushes any pending schedule and dispatches Restart. Go starts
    /// a disabled job goroutine before sending this action; Rust's actor stays
    /// alive and accepts the same Restart action while disabled.
    #[tokio::test]
    async fn test_restart_disabled_mirror_flushes_schedule_and_sends_restart() {
        let (state, mut rx) = state_with_job("archlinux");
        {
            let jobs = state.jobs.read().await;
            jobs.get("archlinux").unwrap().set_state(STATE_DISABLED);
        }
        schedule_job(&state, "archlinux").await;

        let (status, body) = post_cmd(
            Arc::clone(&state),
            worker_cmd("archlinux", CmdVerb::Restart),
        )
        .await;

        assert_eq!(status, 200);
        assert_eq!(body["msg"], "OK");
        assert_schedule_flushed(&state, "archlinux").await;
        expect_action(&mut rx, CtrlAction::Restart).await;
    }

    /// Ping is Go-compatible no-op bookkeeping: it flushes schedule and
    /// returns OK without sending anything to the job control channel.
    #[tokio::test]
    async fn test_ping_known_mirror_flushes_schedule_without_action() {
        let (state, mut rx) = state_with_job("archlinux");
        schedule_job(&state, "archlinux").await;

        let (status, body) =
            post_cmd(Arc::clone(&state), worker_cmd("archlinux", CmdVerb::Ping)).await;

        assert_eq!(status, 200);
        assert_eq!(body["msg"], "OK");
        assert_schedule_flushed(&state, "archlinux").await;
        expect_no_action(&mut rx).await;
    }

    /// Worker-level Reload (empty mirror_id) → 200 {"msg": "Reload triggered"}
    #[tokio::test]
    async fn test_reload_worker_level_returns_reload_triggered() {
        let (reload_tx, mut reload_rx) = mpsc::channel(1);
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

    /// Repeated worker-level Reload requests coalesce into a single pending
    /// queue entry while preserving the same success response.
    #[tokio::test]
    async fn test_reload_worker_level_coalesces_pending_requests() {
        let (reload_tx, mut reload_rx) = mpsc::channel(1);
        let state = Arc::new(AppState {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            schedule_queue: Arc::new(Mutex::new(ScheduleQueue::new())),
            reload_tx: Some(reload_tx),
        });

        let (status1, body1) = post_cmd(Arc::clone(&state), worker_cmd("", CmdVerb::Reload)).await;
        let (status2, body2) = post_cmd(state, worker_cmd("", CmdVerb::Reload)).await;

        assert_eq!(status1, 200);
        assert_eq!(body1["msg"], "Reload triggered");
        assert_eq!(status2, 200);
        assert_eq!(body2["msg"], "Reload triggered");
        assert!(
            reload_rx.recv().await.is_some(),
            "first reload must enqueue one pending request"
        );
        assert!(
            !matches!(
                timeout(Duration::from_millis(50), reload_rx.recv()).await,
                Ok(Some(()))
            ),
            "second reload should be coalesced instead of enqueuing another request"
        );
    }
}

use hustsync_internal::status::SyncStatus;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};

use crate::JobMessage;
use crate::provider::{MirrorProvider, ProviderError};

#[derive(Clone, Copy)]
#[allow(clippy::enum_variant_names)]
enum PrePhase {
    PreJob,
    PreExec,
}

#[derive(Clone, Copy, Debug)]
#[allow(clippy::enum_variant_names)]
enum PostPhase {
    PostExec,
    PostSuccess,
    PostFail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CtrlAction {
    Start,
    Stop,       // stop syncing keep the job
    Disable,    // disable the job (stops goroutine)
    Restart,    // restart syncing
    Ping,       // ensure the goroutine is alive
    Halt,       // worker halts
    ForceStart, // ignore concurrent limit
}

pub const STATE_NONE: u32 = 0; // empty state
pub const STATE_READY: u32 = 1; // ready to run, able to schedule
pub const STATE_PAUSED: u32 = 2; // paused by jobStop
pub const STATE_DISABLED: u32 = 3; // disabled by jobDisable
pub const STATE_HALTING: u32 = 4; // worker is halting

#[derive(Clone)]
pub struct MirrorJob {
    pub name: String,
    pub tx: mpsc::Sender<CtrlAction>,
    pub state: Arc<AtomicU32>,
    pub disabled: Arc<tokio::sync::Notify>,
    pub interval: Duration,
}

impl MirrorJob {
    pub fn state(&self) -> u32 {
        self.state.load(Ordering::Acquire)
    }

    pub fn set_state(&self, state: u32) {
        self.state.store(state, Ordering::Release);
    }

    pub async fn send_ctrl(
        &self,
        action: CtrlAction,
    ) -> Result<(), mpsc::error::SendError<CtrlAction>> {
        self.tx.send(action).await
    }
}

pub struct JobActor {
    pub name: String,
    pub rx: mpsc::Receiver<CtrlAction>,
    pub state: Arc<AtomicU32>,
    pub disabled: Arc<tokio::sync::Notify>,
    pub manager_tx: mpsc::Sender<JobMessage>,
    pub semaphore: Arc<tokio::sync::Semaphore>,
    pub provider: Arc<dyn MirrorProvider>,
    pub hooks: Arc<Vec<Arc<dyn crate::hooks::JobHook>>>,
}

struct RunningJob {
    done: tokio::task::JoinHandle<Result<(), ProviderError>>,
}

impl JobActor {
    pub fn new(
        name: String,
        manager_tx: mpsc::Sender<JobMessage>,
        semaphore: Arc<tokio::sync::Semaphore>,
        provider: Box<dyn MirrorProvider>,
        hooks: Vec<Arc<dyn crate::hooks::JobHook>>,
    ) -> (MirrorJob, Self) {
        let (tx, rx) = mpsc::channel(32);
        let state = Arc::new(AtomicU32::new(STATE_NONE));
        let disabled = Arc::new(tokio::sync::Notify::new());
        let interval = provider.interval();

        let job = MirrorJob {
            name: name.clone(),
            tx,
            state: Arc::clone(&state),
            disabled: Arc::clone(&disabled),
            interval,
        };

        let actor = JobActor {
            name,
            rx,
            state,
            disabled,
            manager_tx,
            semaphore,
            provider: Arc::from(provider),
            hooks: Arc::new(hooks),
        };

        (job, actor)
    }

    fn make_hook_ctx(
        name: &str,
        provider: &Arc<dyn MirrorProvider>,
        attempt: u32,
    ) -> crate::hooks::HookCtx {
        crate::hooks::HookCtx {
            mirror_name: name.to_string(),
            working_dir: provider.working_dir().to_path_buf(),
            upstream_url: provider.upstream().to_string(),
            log_dir: provider.log_dir().to_path_buf(),
            log_file: provider.log_file().to_path_buf(),
            attempt,
            env: std::collections::HashMap::new(),
        }
    }

    /// Run `pre_*` hooks in config order. Returns `Ok(n)` where `n` is
    /// the number of hooks that ran successfully (for LIFO teardown on
    /// failure), or `Err((n, e))` carrying both the completed count and
    /// the error that aborted the pipeline.
    async fn run_pre(
        phase: PrePhase,
        hooks: &[Arc<dyn crate::hooks::JobHook>],
        ctx: &mut crate::hooks::HookCtx,
    ) -> Result<usize, (usize, crate::hooks::HookError)> {
        for (i, hook) in hooks.iter().enumerate() {
            let res = match phase {
                PrePhase::PreJob => hook.pre_job(ctx).await,
                PrePhase::PreExec => hook.pre_exec(ctx).await,
            };
            if let Err(e) = res {
                return Err((i, e));
            }
        }
        Ok(hooks.len())
    }

    /// Run `post_*` hooks LIFO over the first `count` hooks (config
    /// order, reversed). Errors from post-hooks are logged but never
    /// surface back to the caller — Spec §3.2.
    async fn run_post(
        phase: PostPhase,
        hooks: &[Arc<dyn crate::hooks::JobHook>],
        count: usize,
        ctx: &mut crate::hooks::HookCtx,
    ) {
        for hook in hooks.iter().take(count).rev() {
            let res = match phase {
                PostPhase::PostExec => hook.post_exec(ctx).await,
                PostPhase::PostSuccess => hook.post_success(ctx).await,
                PostPhase::PostFail => hook.post_fail(ctx).await,
            };
            if let Err(e) = res {
                tracing::warn!("hook {} failed in {:?}: {}", hook.name(), phase, e);
            }
        }
    }

    async fn report_status(
        manager_tx: &mpsc::Sender<JobMessage>,
        name: &str,
        status: SyncStatus,
        msg: String,
        schedule: bool,
        provider: &Arc<dyn MirrorProvider>,
    ) {
        let _ = manager_tx
            .send(JobMessage {
                status,
                name: name.to_string(),
                msg,
                schedule,
                upstream: provider.upstream().to_string(),
                size: provider.data_size().await,
                is_master: provider.is_master(),
            })
            .await;
    }

    /// Narrow shim between the retry loop and the provider trait.
    ///
    /// Collects every call-site parameter that the provider contract's
    /// `RunContext` will carry (attempt index, cancellation token, env
    /// overrides) in one function so that when the trait signature grows
    /// the diff is contained here.
    async fn invoke_provider(
        provider: &dyn MirrorProvider,
        attempt: u32,
        env: std::collections::HashMap<String, String>,
    ) -> Result<(), ProviderError> {
        use crate::provider::RunContext;
        let ctx = RunContext {
            attempt,
            env,
            ..RunContext::default()
        };
        provider.run(ctx).await
    }

    #[allow(clippy::cognitive_complexity)]
    async fn run_sync_loop(
        name: String,
        provider: Arc<dyn MirrorProvider>,
        semaphore: Arc<tokio::sync::Semaphore>,
        manager_tx: mpsc::Sender<JobMessage>,
        state: Arc<AtomicU32>,
        hooks: Arc<Vec<Arc<dyn crate::hooks::JobHook>>>,
        force: bool,
    ) -> Result<(), ProviderError> {
        // 1. Acquire semaphore (Concurrency control)
        let _permit = if !force {
            tracing::debug!("Job {} waiting for semaphore...", name);
            Some(
                semaphore
                    .acquire_owned()
                    .await
                    .map_err(|_| ProviderError::Execution {
                        code: -1,
                        msg: "semaphore closed".into(),
                    })?,
            )
        } else {
            tracing::info!("Job {} bypassing semaphore (ForceStart)", name);
            None
        };

        // 2. pre_job — once, outside the retry loop (§3 pipeline).
        let mut hook_ctx = Self::make_hook_ctx(&name, &provider, 0);
        let pre_job_count = match Self::run_pre(PrePhase::PreJob, &hooks, &mut hook_ctx).await {
            Ok(n) => n,
            Err((n, e)) => {
                tracing::warn!("Job {} pre_job hook aborted: {}", name, e);
                Self::run_post(PostPhase::PostFail, &hooks, n, &mut hook_ctx).await;
                return Err(ProviderError::Execution {
                    code: -1,
                    msg: format!("pre_job hook failed: {e}"),
                });
            }
        };

        // 3. Retry Loop
        let retries = provider.retry();
        for i in 0..retries {
            if i > 0 {
                tracing::info!("Job {} retrying sync (attempt {}/{})", name, i + 1, retries);
                sleep(Duration::from_secs(2)).await;
            }
            hook_ctx.attempt = i + 1;

            Self::report_status(
                &manager_tx,
                &name,
                SyncStatus::PreSyncing,
                "".into(),
                false,
                &provider,
            )
            .await;

            // 3a. pre_exec — may rotate log files, mutate ctx.env / ctx.log_file.
            let pre_exec_count = match Self::run_pre(PrePhase::PreExec, &hooks, &mut hook_ctx).await
            {
                Ok(n) => n,
                Err((n, e)) => {
                    tracing::warn!("Job {} pre_exec hook aborted: {}", name, e);
                    Self::run_post(PostPhase::PostFail, &hooks, n, &mut hook_ctx).await;
                    Self::run_post(PostPhase::PostFail, &hooks, pre_job_count, &mut hook_ctx).await;
                    return Err(ProviderError::Execution {
                        code: -1,
                        msg: format!("pre_exec hook failed: {e}"),
                    });
                }
            };

            Self::report_status(
                &manager_tx,
                &name,
                SyncStatus::Syncing,
                "".into(),
                false,
                &provider,
            )
            .await;

            // 3b. Provider run with hook-injected env.
            let provider_env = hook_ctx.env.clone();
            let run_result = Self::invoke_provider(provider.as_ref(), i, provider_env).await;

            // 3c. post_exec always runs, regardless of success/fail.
            Self::run_post(PostPhase::PostExec, &hooks, pre_exec_count, &mut hook_ctx).await;

            match run_result {
                Ok(_) => {
                    tracing::info!("Job {} sync succeeded", name);
                    Self::run_post(
                        PostPhase::PostSuccess,
                        &hooks,
                        pre_exec_count,
                        &mut hook_ctx,
                    )
                    .await;
                    let is_ready = state.load(Ordering::Acquire) == STATE_READY;
                    Self::report_status(
                        &manager_tx,
                        &name,
                        SyncStatus::Success,
                        "".into(),
                        is_ready,
                        &provider,
                    )
                    .await;
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!("Job {} sync failed: {}", name, e);
                    Self::run_post(PostPhase::PostFail, &hooks, pre_exec_count, &mut hook_ctx)
                        .await;

                    let current_state = state.load(Ordering::Acquire);
                    if current_state == STATE_PAUSED || current_state == STATE_DISABLED {
                        tracing::info!(
                            "Job {} was terminated by user (state {}), not reporting as Failed.",
                            name,
                            current_state
                        );
                        return Err(ProviderError::Terminated);
                    }

                    let is_last_retry = i == retries - 1;
                    let is_ready = current_state == STATE_READY;

                    Self::report_status(
                        &manager_tx,
                        &name,
                        SyncStatus::Failed,
                        e.to_string(),
                        is_last_retry && is_ready,
                        &provider,
                    )
                    .await;

                    if let ProviderError::Terminated = e {
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }

    fn spawn_run(&self, force: bool) -> RunningJob {
        let name = self.name.clone();
        let provider = Arc::clone(&self.provider);
        let semaphore = Arc::clone(&self.semaphore);
        let manager_tx = self.manager_tx.clone();
        let state = Arc::clone(&self.state);
        let hooks = Arc::clone(&self.hooks);

        let done = tokio::spawn(async move {
            Self::run_sync_loop(name, provider, semaphore, manager_tx, state, hooks, force).await
        });

        RunningJob { done }
    }

    #[allow(clippy::cognitive_complexity)]
    pub async fn run(mut self) {
        tracing::debug!("Job actor {} started", self.name);

        let mut running: Option<RunningJob> = None;
        let mut force_next = false;

        loop {
            if let Some(r) = &mut running {
                tokio::select! {
                    res = &mut r.done => {
                        running = None;
                        let sync_result = res.unwrap_or(Err(ProviderError::Terminated));
                        match sync_result {
                            Ok(_) => {
                                self.state.store(STATE_NONE, Ordering::Release);
                            }
                            Err(ProviderError::Terminated) => {}
                            Err(_) => {
                                self.state.store(STATE_NONE, Ordering::Release);
                            }
                        }
                    }
                    Some(ctrl) = self.rx.recv() => {
                        match ctrl {
                            CtrlAction::Stop => {
                                self.state.store(STATE_PAUSED, Ordering::Release);
                                let _ = self.provider.terminate().await;
                                if let Some(r) = running.take() {
                                    let _ = r.done.await;
                                }
                            }
                            CtrlAction::Disable => {
                                self.state.store(STATE_DISABLED, Ordering::Release);
                                let _ = self.provider.terminate().await;
                                if let Some(r) = running.take() {
                                    let _ = r.done.await;
                                }
                                self.disabled.notify_waiters();
                            }
                            CtrlAction::Restart => {
                                tracing::info!("Job {} restarting, terminating current process...", self.name);
                                let _ = self.provider.terminate().await;
                                if let Some(r) = running.take() {
                                    let _ = r.done.await;
                                }
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                self.state.store(STATE_READY, Ordering::Release);
                                running = Some(self.spawn_run(false));
                            }
                            CtrlAction::ForceStart => {
                                tracing::warn!("Job {} received ForceStart while already running. Treating as deferred.", self.name);
                                force_next = true;
                            }
                            CtrlAction::Halt => {
                                self.state.store(STATE_HALTING, Ordering::Release);
                                let _ = self.provider.terminate().await;
                                if let Some(r) = running.take() {
                                    let _ = r.done.await;
                                }
                                return;
                            }
                            _ => {}
                        }
                    }
                }
            } else {
                match self.rx.recv().await {
                    Some(CtrlAction::Start) | Some(CtrlAction::Restart) => {
                        self.state.store(STATE_READY, Ordering::Release);
                        let force = force_next;
                        force_next = false;
                        running = Some(self.spawn_run(force));
                    }
                    Some(CtrlAction::ForceStart) => {
                        self.state.store(STATE_READY, Ordering::Release);
                        force_next = false;
                        running = Some(self.spawn_run(true));
                    }
                    Some(CtrlAction::Stop) => {
                        self.state.store(STATE_PAUSED, Ordering::Release);
                    }
                    Some(CtrlAction::Disable) => {
                        self.state.store(STATE_DISABLED, Ordering::Release);
                        self.disabled.notify_waiters();
                    }
                    Some(CtrlAction::Halt) => {
                        self.state.store(STATE_HALTING, Ordering::Release);
                        return;
                    }
                    Some(CtrlAction::Ping) => {}
                    None => break,
                }
            }
        }
        tracing::debug!("Job actor {} exited", self.name);
    }
}

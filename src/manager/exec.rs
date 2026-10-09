//! Runs a command inside a running workload, once per idempotency key. The workload's own
//! output over time is `logs.rs`.

use super::*;

/// Exec output kept per stream; the rest is dropped and flagged.
const MAX_EXEC_OUTPUT: usize = 1024 * 1024;
const IDEMPOTENCY_TTL: Duration = Duration::from_secs(600);
const IDEMPOTENCY_MAX: usize = 1024;

impl Manager {
    // ─── exec ──────────────────────────────────────────────────────────────────────────────

    pub async fn exec(
        self: &Arc<Self>,
        id: WorkloadId,
        request: ExecRequest,
        key: Option<String>,
        epoch: Option<u64>,
    ) -> Result<ExecResponse, NodeError> {
        request.validate().map_err(NodeError::Invalid)?;
        check_epoch(&self.record(&id)?, epoch, EpochRule::Exact)?;
        let Some(key) = key else { return self.exec_once(&id, &request, epoch).await };
        let fingerprint = request.fingerprint(&id);
        let cell = {
            let mut cache = self.exec_keys.lock().unwrap();
            let now = Instant::now();
            cache.entries.retain(|_, (_, _, at)| now.duration_since(*at) < IDEMPOTENCY_TTL);
            if cache.entries.len() >= IDEMPOTENCY_MAX
                && let Some(oldest) = cache.entries.iter().min_by_key(|(_, (_, _, at))| *at).map(|(k, _)| k.clone())
            {
                cache.entries.remove(&oldest);
            }
            match cache.entries.get(&key) {
                Some((fp, cell, _)) if *fp == fingerprint => cell.clone(),
                Some(_) => return Err(NodeError::IdempotencyMismatch),
                None => {
                    let cell = Arc::new(OnceCell::new());
                    cache.entries.insert(key.clone(), (fingerprint, cell.clone(), now));
                    cell
                }
            }
        };
        // One run per key; a duplicate waits for it and gets its answer. Failures aren't cached,
        // so a retry after one runs the command again.
        let response = cell.get_or_try_init(|| self.exec_once(&id, &request, epoch)).await?;
        Ok(response.clone())
    }

    async fn exec_once(
        &self,
        id: &WorkloadId,
        request: &ExecRequest,
        epoch: Option<u64>,
    ) -> Result<ExecResponse, NodeError> {
        let started = Instant::now();
        let result = async {
            let container = self.exec_target(id, epoch).await?;
            let timeout = Duration::from_secs(request.timeout_seconds as u64);
            // By the container's id, never its name: a workload replaced meanwhile has a new
            // container under the same name, and the command must not run there.
            let out = match self.note(self.runtime.exec(&container, &request.command, timeout, MAX_EXEC_OUTPUT).await) {
                Ok(out) => out,
                Err(RuntimeError::NotFound(_) | RuntimeError::Conflict(_)) => {
                    return Err(NodeError::Conflict {
                        code: ConflictCode::NotRunning,
                        message: "the workload stopped or was replaced before the command ran".into(),
                    });
                }
                Err(e) => return Err(e.into()),
            };
            Ok(ExecResponse {
                exit_code: out.exit_code,
                stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
                stdout_truncated: out.stdout_truncated,
                stderr_truncated: out.stderr_truncated,
                timed_out: out.timed_out,
                killed: out.killed,
                duration_ms: started.elapsed().as_millis() as u64,
            })
        }
        .await;
        self.metrics.operation("exec", &result, started.elapsed());
        result
    }

    /// The container an exec runs in, checked under the workload's lock: the current copy's, and
    /// running. The lock isn't held for the command itself, which may take minutes and must not
    /// hold up a stop or a fence.
    async fn exec_target(&self, id: &WorkloadId, epoch: Option<u64>) -> Result<String, NodeError> {
        let lock = self.lock_for(id);
        let _guard = lock.lock().await;
        let record = self.record(id)?;
        check_epoch(&record, epoch, EpochRule::Exact)?;
        let info = if record.phase == Phase::Active { self.inspect(&record.container_name).await? } else { None };
        match info {
            Some(info) if info.status == ContainerStatus::Running => Ok(info.id),
            _ => Err(NodeError::Conflict {
                code: ConflictCode::NotRunning,
                message: "exec needs a running workload".into(),
            }),
        }
    }
}

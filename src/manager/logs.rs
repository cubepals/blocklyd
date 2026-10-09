//! Reads a workload's log as NDJSON records, following it across restarts when asked. Commands
//! run inside the workload are `exec.rs`.

use super::*;

const MAX_LOG_LINE: usize = 16 * 1024;
const MAX_LOG_TAIL: u32 = 10_000;

impl Manager {
    // ─── logs ──────────────────────────────────────────────────────────────────────────────

    /// NDJSON records. Following continues across the workload's restarts and ends when its
    /// container is removed or the client goes away.
    pub fn logs(
        self: &Arc<Self>,
        id: &WorkloadId,
        tail: Option<u32>,
        since: Option<i64>,
        follow: bool,
    ) -> Result<BoxStream<'static, LogRecord>, NodeError> {
        let record = self.record(id)?;
        if record.phase == Phase::Retained {
            return Err(NodeError::Conflict {
                code: ConflictCode::NoCompute,
                message: "a decommissioned workload has no logs".into(),
            });
        }
        let tail = tail.map(|t| t.min(MAX_LOG_TAIL)).or(if follow { Some(0) } else { Some(200) });
        let (tx, mut rx) = mpsc::channel::<LogRecord>(256);
        let this = self.clone();
        let name = record.container_name.clone();
        tokio::spawn(async move {
            let mut options = LogOptions { tail, since_unix: since, follow };
            let mut last_start = this.runtime.inspect(&name).await.ok().flatten().and_then(|i| i.started_at);
            loop {
                let mut stream = this.runtime.logs(&name, options.clone());
                loop {
                    // A client gone while the workload prints nothing is noticed now, not at its
                    // next line, which on an idle server may be hours away.
                    let item = tokio::select! {
                        item = stream.next() => item,
                        () = tx.closed() => return,
                    };
                    let Some(item) = item else { break };
                    match item {
                        Ok(chunk) => {
                            for record in lines_of(chunk.stream, &chunk.bytes) {
                                if tx.send(record).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(LogRecord::Event { event: "error".into(), reason: e.to_string() }).await;
                            return;
                        }
                    }
                }
                if !follow {
                    let _ = tx.send(LogRecord::Event { event: "end".into(), reason: "complete".into() }).await;
                    return;
                }
                // The run ended. Wait for the next one, then read it from its start.
                loop {
                    if tx.is_closed() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    match this.runtime.inspect(&name).await {
                        Ok(None) => {
                            let _ = tx.send(LogRecord::Event { event: "end".into(), reason: "removed".into() }).await;
                            return;
                        }
                        Ok(Some(info))
                            if info.started_at.is_some()
                                && info.started_at != last_start
                                && info.status == ContainerStatus::Running =>
                        {
                            last_start = info.started_at;
                            let _ =
                                tx.send(LogRecord::Event { event: "restarted".into(), reason: "new run".into() }).await;
                            options = LogOptions {
                                tail: None,
                                since_unix: info.started_at.map(OffsetDateTime::unix_timestamp),
                                follow: true,
                            };
                            break;
                        }
                        Ok(Some(_)) => {}
                        Err(e) if e.is_unavailable() => {
                            let _ = tx.send(LogRecord::Event { event: "error".into(), reason: e.to_string() }).await;
                            return;
                        }
                        Err(_) => {}
                    }
                }
            }
        });
        Ok(futures_util::stream::poll_fn(move |cx| rx.poll_recv(cx)).boxed())
    }
}

/// Splits a runtime log chunk into NDJSON records: `<rfc3339> <line>` per line.
pub(crate) fn lines_of(stream: LogStream, bytes: &[u8]) -> Vec<LogRecord> {
    let text = String::from_utf8_lossy(bytes);
    let stream = match stream {
        LogStream::Stdout => "stdout",
        LogStream::Stderr => "stderr",
    };
    text.split('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        .filter(|line| !line.is_empty())
        .map(|line| {
            let (ts, rest) = match line.split_once(' ') {
                Some((ts, rest)) if parse_time(Some(ts)).is_some() => (Some(ts.to_owned()), rest),
                _ => (None, line),
            };
            let mut rest = rest.to_owned();
            if rest.len() > MAX_LOG_LINE {
                let mut cut = MAX_LOG_LINE;
                while !rest.is_char_boundary(cut) {
                    cut -= 1;
                }
                rest.truncate(cut);
                rest.push('…');
            }
            LogRecord::Line { ts, stream: stream.into(), line: rest }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_chunks_become_records() {
        let chunk = b"2026-09-28T02:16:22.123456789Z [Server thread/INFO]: Done (9.983s)!\r\nno timestamp here\n\n";
        let records = lines_of(LogStream::Stdout, chunk);
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0],
            LogRecord::Line {
                ts: Some("2026-09-28T02:16:22.123456789Z".into()),
                stream: "stdout".into(),
                line: "[Server thread/INFO]: Done (9.983s)!".into()
            }
        );
        assert!(matches!(&records[1], LogRecord::Line { ts: None, .. }));
        let long = format!("2026-09-28T02:16:22Z {}", "é".repeat(20_000));
        let LogRecord::Line { line, .. } = &lines_of(LogStream::Stderr, long.as_bytes())[0] else { panic!() };
        assert!(line.len() <= MAX_LOG_LINE + '…'.len_utf8());
    }
}

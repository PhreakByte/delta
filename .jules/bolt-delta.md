## 2025-12-25 - [Async Checkpointing]
**Learning:**
- Synchronous checkpointing adds significant latency spikes (50-100% overhead) every N commits.
- Commit 10 took ~2280ms vs ~1100ms average with sync checkpointing.
- Commit 10 took ~940ms with async checkpointing.
- Async checkpointing moved the latency from P99 to background, smoothing out commit times.
- `CheckpointHook` can leverage `SnapshotManagement.deltaLogAsyncUpdateThreadPool` effectively.

**Action:**
- Introduced `spark.databricks.delta.checkpoint.async.enabled` to control this behavior.
- Use `CheckpointHook` to submit checkpoint tasks to the thread pool.
- This optimization is safe as `deltaLog.checkpoint` is thread-safe (writes to unique files).

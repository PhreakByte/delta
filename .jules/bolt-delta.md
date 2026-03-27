# BOLT'S JOURNAL - CRITICAL LEARNINGS ONLY

## 2025-12-24 - [ASYNC CHECKPOINT OPTIMIZATION]
**Learning:** Post-commit checkpointing (every 10 commits) is synchronous and blocking, causing significant latency spikes.
**Action:** Implemented `delta.checkpoint.async.enabled` configuration. Modified `CheckpointHook` to use a `newDaemonCachedThreadPool` to run checkpointing in the background. Validated with `CheckpointPerformanceSuite` that checkpoints are eventually written without blocking the commit. This eliminates the "checkpoint tax" on writers.

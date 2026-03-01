## 2025-12-23 - [Optimize HDFSLogStore Commit Latency]
**Learning:** Found redundant `exists` RPC call in `HDFSLogStore.write` for non-local file systems. This adds latency to every commit on HDFS/S3.
**Action:** Removed `exists` check for non-local FS, relying on atomic `rename` failure instead. Verified with unit tests.

/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.hooks

import org.apache.spark.sql.delta.CommittedTransaction
import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging
import org.apache.spark.util.ThreadUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf

/** Write a new checkpoint at the version committed by the txn if required. */
object CheckpointHook extends PostCommitHook with Logging {
  override val name: String = "Post commit checkpoint trigger"

  private val checkpointExecutor = ThreadUtils.newDaemonCachedThreadPool("delta-checkpoint", 16)

  override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
    if (!txn.needsCheckpoint) return

    val asyncEnabled = spark.conf.get(DeltaSQLConf.DELTA_CHECKPOINT_ASYNC_ENABLED)

    // Since the postCommitSnapshot isn't guaranteed to match committedVersion, we have to
    // explicitly checkpoint the snapshot at the committedVersion.
    val cp = txn.postCommitSnapshot.checkpointProvider
    val snapshotToCheckpoint = txn.deltaLog.getSnapshotAt(
      txn.committedVersion,
      lastCheckpointHint = None,
      lastCheckpointProvider = Some(cp),
      catalogTableOpt = txn.catalogTable,
      enforceTimeTravelWithinDeletedFileRetention = false)

    if (asyncEnabled) {
      checkpointExecutor.submit(new Runnable {
        override def run(): Unit = {
          try {
            // Set the active Spark Session for this thread as DeltaLog methods rely on it
            SparkSession.setActiveSession(spark)
            txn.deltaLog.checkpoint(snapshotToCheckpoint, txn.catalogTable)
          } catch {
            case t: Throwable =>
              logWarning(s"Async checkpoint failed for ${txn.deltaLog.logPath}", t)
          } finally {
            SparkSession.clearActiveSession()
          }
        }
      })
    } else {
      txn.deltaLog.checkpoint(snapshotToCheckpoint, txn.catalogTable)
    }
  }
}

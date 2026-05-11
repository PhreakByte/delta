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

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{CommittedTransaction, SnapshotManagement}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession

/** Write a new checkpoint at the version committed by the txn if required. */
object CheckpointHook extends PostCommitHook with DeltaLogging {
  override val name: String = "Post commit checkpoint trigger"

  override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
    if (!txn.needsCheckpoint) return

    // Since the postCommitSnapshot isn't guaranteed to match committedVersion, we have to
    // explicitly checkpoint the snapshot at the committedVersion.
    val cp = txn.postCommitSnapshot.checkpointProvider
    val snapshotToCheckpoint = txn.deltaLog.getSnapshotAt(
      txn.committedVersion,
      lastCheckpointHint = None,
      lastCheckpointProvider = Some(cp),
      catalogTableOpt = txn.catalogTable,
      enforceTimeTravelWithinDeletedFileRetention = false)

    if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CHECKPOINT_ASYNC_ENABLED)) {
      try {
        val jobGroup = spark.sparkContext.getLocalProperty(
          org.apache.spark.SparkContext.SPARK_JOB_GROUP_ID)
        SnapshotManagement.deltaLogAsyncUpdateThreadPool.submit(spark) {
          spark.sparkContext.setLocalProperty("spark.scheduler.pool", "deltaStateUpdatePool")
          spark.sparkContext.setJobGroup(
            jobGroup,
            s"Asynchronously checkpointing Delta table at ${txn.deltaLog.dataPath}",
            interruptOnCancel = true)
          SparkSession.setActiveSession(spark)
          try {
            txn.deltaLog.checkpoint(snapshotToCheckpoint, txn.catalogTable)
          } catch {
            case NonFatal(e) =>
              recordDeltaEvent(
                txn.deltaLog,
                "delta.checkpoint.async.error",
                data = Map("exception" -> e.getMessage))
          }
        }
      } catch {
        case NonFatal(e) =>
          recordDeltaEvent(
            txn.deltaLog,
            "delta.checkpoint.async.submit.error",
            data = Map("exception" -> e.getMessage))
          // Fallback to synchronous checkpointing if async submission fails
          txn.deltaLog.checkpoint(snapshotToCheckpoint, txn.catalogTable)
      }
    } else {
      txn.deltaLog.checkpoint(snapshotToCheckpoint, txn.catalogTable)
    }
  }
}

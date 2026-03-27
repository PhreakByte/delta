package org.apache.spark.sql.delta

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.scalatest.concurrent.Eventually
import org.scalatest.time.Span
import org.scalatest.time.Seconds

class CheckpointPerformanceSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with Eventually {

  import testImplicits._

  test("checkpointing should be asynchronous when enabled") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      // Initialize table
      spark.range(10).write.format("delta").save(path)

      // Configure async checkpointing and interval=1
      spark.conf.set(DeltaSQLConf.DELTA_CHECKPOINT_ASYNC_ENABLED.key, "true")
      spark.sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.checkpointInterval' = '1')")

      // Perform a commit
      spark.range(10).write.format("delta").mode("append").save(path)

      // Verify checkpoint is eventually written
      eventually(timeout(Span(30, Seconds))) {
        val deltaLog = DeltaLog.forTable(spark, path)
        val lastCheckpoint = deltaLog.readLastCheckpointFile()
        assert(lastCheckpoint.isDefined, "Checkpoint should eventually be written")
        assert(lastCheckpoint.get.version == 2, s"Checkpoint should be at version 2, but was ${lastCheckpoint.get.version}")
      }
    }
  }
}

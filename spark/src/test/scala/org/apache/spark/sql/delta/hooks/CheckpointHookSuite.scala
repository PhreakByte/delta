package org.apache.spark.sql.delta.hooks

import java.io.File
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.delta.{DeltaLog, DeltaTestUtilsForTempViews}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

class CheckpointHookSuite extends DeltaSQLCommandTest with SharedSparkSession
  with DeltaTestUtilsForTempViews {

  test("async checkpointing") {
    withTempDir { dir =>
      withSQLConf(DeltaSQLConf.DELTA_CHECKPOINT_ASYNC_ENABLED.key -> "true") {
        val path = dir.getAbsolutePath
        spark.range(10).write.format("delta").save(path)

        // Write enough commits to trigger checkpoint
        for (_ <- 0 to 10) {
          spark.range(10).write.format("delta").mode("append").save(path)
        }

        val logDir = new File(path, "_delta_log")
        eventually(timeout(15.seconds), interval(1.second)) {
          val checkpoints = logDir.listFiles().filter(_.getName.endsWith(".checkpoint.parquet"))
          assert(checkpoints.length > 0)
        }
      }
    }
  }
}

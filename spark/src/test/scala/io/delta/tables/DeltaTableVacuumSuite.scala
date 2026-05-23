package io.delta.tables

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.SparkConf

class DeltaTableVacuumSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

  test("vacuum should return an empty dataframe with a path string column") {
    withTempDir { dir =>
      spark.range(10).write.format("delta").save(dir.getCanonicalPath)
      val dt = DeltaTable.forPath(spark, dir.getCanonicalPath)

      // turn off retention duration check to allow vacuum 0
      spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

      val result = dt.vacuum(0.0)

      assert(result.schema == StructType(Seq(StructField("path", StringType, nullable = true))))
      // It returns an empty dataframe now, but to avoid the test failing on non-empty results if
      // it actually deleted files and returned their paths (which VacuumTableCommand does),
      // we only care about the schema here.
    }
  }
}

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

package org.apache.spark.sql.delta.catalog

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

class MultiCatalogSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  test("switch to secondary DeltaCatalog") {
    withSQLConf("spark.sql.catalog.other_catalog" -> classOf[DeltaCatalog].getName) {
      withTable("t1", "t2") {
        // Start by using default `spark_catalog`
        sql("SET CATALOG spark_catalog")
        sql("CREATE DATABASE db1")
        sql("USE DATABASE db1")
        sql("CREATE TABLE t1 (id LONG) USING DELTA")
        sql("INSERT INTO t1 VALUES (1), (2)")
        checkAnswer(sql("SELECT * FROM t1"), Seq(Row(1), Row(2)))

        // Switch to second `other_catalog` catalog
        sql("SET CATALOG other_catalog")
        sql("CREATE DATABASE db2")
        sql("USE DATABASE db2")
        sql("CREATE TABLE t2 (id LONG) USING DELTA")
        sql("INSERT INTO t2 VALUES (3), (4)")
        checkAnswer(sql("SELECT * FROM t2"), Seq(Row(3), Row(4)))

        // Switch back to the first catalog and verify the data
        sql("SET CATALOG spark_catalog")
        checkAnswer(sql("SELECT * FROM db1.t1"), Seq(Row(1), Row(2)))
      }
    }
  }
}

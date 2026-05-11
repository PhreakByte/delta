from pyspark.sql import SparkSession
from delta import *

spark = (
    SparkSession.builder.appName("test_checkpoint_async")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sql("CREATE TABLE test_checkpoint (id INT) USING delta")
spark.sql("INSERT INTO test_checkpoint VALUES (1)")
spark.sql("INSERT INTO test_checkpoint VALUES (2)")

print("Done")

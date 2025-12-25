import time
import os
import shutil
from pyspark.sql import SparkSession
from delta import *

def cleanup_dir(path):
    if os.path.exists(path):
        shutil.rmtree(path)

# Initialize Spark
builder = SparkSession.builder     .appName("DeltaLogPerf")     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")     .config("spark.databricks.delta.properties.defaults.checkpointInterval", "10")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Path for the table
table_path = "/tmp/delta_log_perf_table"
cleanup_dir(table_path)

# Create a simple DataFrame
data = spark.range(0, 1)

print("Starting commits...")
start_time = time.time()
num_commits = 20

# Perform commits
commit_times = []
for i in range(num_commits):
    iter_start = time.time()
    data.write.format("delta").mode("append").save(table_path)
    iter_end = time.time()
    commit_times.append(iter_end - iter_start)
    if i % 10 == 0:
        print(f"Commit {i} completed")

end_time = time.time()
total_time = end_time - start_time

print(f"Total time for {num_commits} commits: {total_time:.2f} seconds")
print(f"Average time per commit: {sum(commit_times) / len(commit_times):.2f} seconds")

# Analyze checkpoint impact
# Every 10th commit should be slower due to checkpointing
for i in range(num_commits):
    is_checkpoint = (i + 1) % 10 == 0
    print(f"Commit {i+1}: {commit_times[i]:.4f}s {'(CHECKPOINT)' if is_checkpoint else ''}")

# Verify log files
log_path = os.path.join(table_path, "_delta_log")
log_files = os.listdir(log_path)
json_files = [f for f in log_files if f.endswith(".json")]
checkpoint_files = [f for f in log_files if f.endswith(".parquet")]

print(f"Number of JSON log files: {len(json_files)}")
print(f"Number of Checkpoint files: {len(checkpoint_files)}")

spark.stop()
cleanup_dir(table_path)

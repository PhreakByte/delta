### Title
[PYTHON] Fix `DeltaTable.vacuum` return type in Delta Connect

### Affected area
- module: `python/delta/connect`
- file: `python/delta/connect/tables.py`
- function or rule name: `vacuum`
- Spark version or branch: master

### Problem statement
The `vacuum` method in the `DeltaTable` API in Spark Connect currently returns `None`, violating the documented `-> DataFrame` return type hint. This causes an `AttributeError` if users attempt to chain standard DataFrame methods (like `.show()`) on the result. The traditional standard PySpark Delta implementation returns an empty DataFrame with a schema of a single string column `path`.

### Reproduction testcase
The testcase covers unit testing the schema to prevent regression.
```python
    def test_vacuum_return_empty_dataframe(self) -> None:
        from pyspark.sql.types import StringType
        from delta.connect.proto.base_pb2 import DeltaTable as PBDeltaTable

        class MockClient:
            def execute_command(self, command):
                pass

        class MockSpark:
            def __init__(self):
                self.client = MockClient()
            def createDataFrame(self, data, schema):
                from pyspark.sql import SparkSession
                spark = SparkSession.builder.getOrCreate()
                return spark.createDataFrame(data, schema)

        dt = DeltaTable(MockSpark())
        dt._to_proto = lambda: PBDeltaTable()
        df = dt.vacuum()
        self.assertEqual(df.count(), 0)
        self.assertEqual(df.schema.names, ['path'])
        self.assertEqual(df.schema['path'].dataType, StringType())
```

### Root cause
The implementation in `DeltaTable.vacuum` in `tables.py` had a missing return behavior and was returning `None` instead of the empty DataFrame matching the exact schema definition it was supposed to have, as indicated by a comment `# TODO: Return empty DataFrame`.

### Fix
Construct an empty DataFrame with the expected schema (`path: StringType`) using `self._spark.createDataFrame([], schema)` and return it.

### Verification
A unit test `test_vacuum_return_empty_dataframe` was added in `python/delta/connect/tests/test_deltatable.py` to verify that `vacuum` returns a DataFrame with 0 count and `path` of `StringType`. The test passes.

### Impact
Improves code correctness and feature parity with traditional Spark API. Users of Spark Connect's Delta API will now be able to reliably consume `vacuum()` and seamlessly compose it with DataFrame methods exactly as they do using standard PySpark API, eliminating runtime exceptions.

### PR-ready summary
Fix `vacuum` method in `DeltaTable` Spark Connect API to return an empty DataFrame with correct schema instead of returning `None`, completing missing feature parity with traditional PySpark implementation.

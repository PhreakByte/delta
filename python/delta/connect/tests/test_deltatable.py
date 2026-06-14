#
# Copyright (2024) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import unittest
import sys

from delta.connect.testing.utils import DeltaTestCase

path_to_delta_connect_tests_folder = os.path.dirname(os.path.abspath(__file__))
path_to_delta_folder = os.path.dirname(os.path.dirname(path_to_delta_connect_tests_folder))
sys.path.append(path_to_delta_folder)

from tests.test_deltatable import DeltaTableTestsMixin


class DeltaTableTests(DeltaTableTestsMixin, DeltaTestCase):
    @unittest.skip("relies on jvm")
    def test_verify_paritionedBy_compatibility(self):
        pass

    def test_vacuum_return_empty_dataframe(self):
        # Setup a delta table and call vacuum
        # Using tempFile from DeltaTestCase
        df = self.spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
        df.write.format("delta").save(self.tempFile)

        from delta.connect.tables import DeltaTable
        dt = DeltaTable.forPath(self.spark, self.tempFile)

        # Test default vacuum
        res = dt.vacuum()
        self.assertIsNotNone(res)

        # Test schema matches structurally expected Empty DataFrame with path column
        from pyspark.sql.types import StructType, StructField, StringType
        expected_schema = StructType([StructField("path", StringType(), True)])
        self.assertEqual(res.schema, expected_schema)
        self.assertEqual(res.collect(), [])



if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)

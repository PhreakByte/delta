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
import warnings

from delta.connect.testing.utils import DeltaTestCase
from pyspark.sql.types import StructType, StructField, StringType

path_to_delta_connect_tests_folder = os.path.dirname(os.path.abspath(__file__))
path_to_delta_folder = os.path.dirname(os.path.dirname(path_to_delta_connect_tests_folder))
sys.path.append(path_to_delta_folder)

from tests.test_deltatable import DeltaTableTestsMixin


class DeltaTableTests(DeltaTableTestsMixin, DeltaTestCase):
    @unittest.skip("relies on jvm")
    def test_verify_paritionedBy_compatibility(self):
        pass

    def test_vacuum_returns_empty_dataframe(self):
        from delta.connect.tables import DeltaTable

        self.spark.sql("CREATE TABLE default.test_table_vacuum (id INT) USING delta")
        dt = DeltaTable.forName(self.spark, "default.test_table_vacuum")
        result_df = dt.vacuum()
        expected_schema = StructType([StructField("path", StringType(), True)])
        self.assertEqual(result_df.schema, expected_schema)


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)

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
from unittest.mock import MagicMock
from delta.connect.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType

path_to_delta_connect_tests_folder = os.path.dirname(os.path.abspath(__file__))
path_to_delta_folder = os.path.dirname(os.path.dirname(path_to_delta_connect_tests_folder))
sys.path.append(path_to_delta_folder)

from tests.test_deltatable import DeltaTableTestsMixin


class DeltaTableTests(DeltaTableTestsMixin, DeltaTestCase):
    @unittest.skip("relies on jvm")
    def test_verify_paritionedBy_compatibility(self):
        pass

    def test_vacuum_return_value(self):
        # Mock SparkSession and its client
        mock_spark = MagicMock()
        mock_client = MagicMock()
        mock_spark.client = mock_client

        # Mock createDataFrame to return a dummy value
        expected_schema = StructType([StructField("path", StringType(), True)])
        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df

        # Instantiate DeltaTable directly to test PySpark Delta Connect client behavior
        # independently of a running Spark Connect server
        dt = DeltaTable(mock_spark, path='/tmp/test')

        # Call vacuum
        result = dt.vacuum()

        # Verify it returns the dataframe
        self.assertEqual(result, mock_df)

        # Verify createDataFrame was called with the correct schema
        mock_spark.createDataFrame.assert_called_once_with([], expected_schema)


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)

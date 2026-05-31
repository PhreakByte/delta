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

    def test_vacuum_return_schema(self):
        # Verify that vacuum on a connect table returns a DataFrame with the right schema
        from unittest import mock
        from pyspark.sql.types import StructType, StructField, StringType
        from delta.connect.tables import DeltaTable

        # We need to test the logic of `vacuum` independently of the running connect server
        # since standard test_vacuum in mixin runs integration tests. We want to be sure
        # we strictly test the unit behavior inside Delta Connect client.
        mock_spark = mock.MagicMock()
        mock_plan = mock.MagicMock()

        connect_table = DeltaTable(mock_spark, path="/tmp/test", plan=mock_plan)
        connect_table._to_proto = mock.MagicMock(return_value=None)

        # Mock spark.createDataFrame
        expected_schema = StructType([StructField("path", StringType(), True)])
        expected_df = mock.MagicMock()
        expected_df.schema = expected_schema
        mock_spark.createDataFrame.return_value = expected_df

        with mock.patch('delta.connect.tables.Vacuum') as mock_vacuum:
            mock_command = mock.MagicMock()
            mock_vacuum.return_value.command.return_value = mock_command

            result = connect_table.vacuum()
            self.assertEqual(result, expected_df)

            mock_spark.createDataFrame.assert_called_once()
            args, kwargs = mock_spark.createDataFrame.call_args
            self.assertEqual(args[0], [])
            self.assertEqual(kwargs['schema'], expected_schema)


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)

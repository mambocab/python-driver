# Copyright 2013-2016 DataStax, Inc.
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

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import mock

from cassandra import timestamps


class TestMonotonicTimestampGenerator(unittest.TestCase):

    @mock.patch('cassandra.timestamps.time')
    def _call_and_check_results(self, patched_time_module, system_times, expected_timestamps):
        patched_time_module.time = mock.Mock()
        patched_time_module.time.side_effect = system_times
        tsg = timestamps.MonotonicTimestampGenerator()

        for ts in expected_timestamps:
            self.assertEqual(tsg(), ts)

        # assert we patched timestamps.time.time correctly
        with self.assertRaises(StopIteration):
            tsg()

    def test_timestamps_during_and_after_same_system_time(self):
        """
        Test that MonotonicTimestampGenerator's output increases by 1 when the
        underlying system time is the same, then returns to normal when the
        system time changes again.
        """
        self._call_and_check_results(
            system_times=(15.0, 15.0, 15.0, 15.01),
            expected_timestamps=(15 * 1e6,
                                 15 * 1e6 + 1,
                                 15 * 1e6 + 2,
                                 15.01 * 1e6)
        )

    @mock.patch('cassandra.timestamps.time')
    def test_timestamps_during_and_after_backwards_system_time(self, patched_time_module):
        """
        Test that MonotonicTimestampGenerator's output increases by 1 when the
        underlying system time goes backward, then returns to normal when the
        system time increases again.
        """
        self._call_and_check_results(
            system_times=(15.0, 13.0, 14.0, 13.5, 15.01),
            expected_timestamps=(15 * 1e6,
                                 15 * 1e6 + 1,
                                 15 * 1e6 + 2,
                                 15 * 1e6 + 3,
                                 15.01 * 1e6)
        )

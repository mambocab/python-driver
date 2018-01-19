# Copyright 2013-2017 DataStax, Inc.
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
    import unittest # noqa

from mock import patch
import weakref
import six

from cassandra.protocol import SupportedMessage, ReadyMessage

from tests import is_monkey_patched
from tests.unit.io.utils import ReactorTestMixin


try:
    from cassandra.io.libevreactor import _cleanup as libev__cleanup
    from cassandra.io.libevreactor import LibevConnection
except ImportError:
    LibevConnection = None  # noqa


class LibevConnectionTest(unittest.TestCase, ReactorTestMixin):

    connection_class = LibevConnection
    socket_attr_name = '_socket'
    null_handle_function_args = None, 0

    def setUp(self):
        if is_monkey_patched():
            raise unittest.SkipTest("Can't test libev with monkey patching")
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        LibevConnection.initialize_reactor()

        # we patch here rather than as a decorator so that the Mixin can avoid
        # specifying patch args to test methods
        patchers = [patch(obj) for obj in
                    ('socket.socket',
                     'cassandra.io.libevwrapper.IO',
                     'cassandra.io.libevwrapper.Prepare',
                     'cassandra.io.libevwrapper.Async',
                     'cassandra.io.libevreactor.LibevLoop.maybe_start')]
        for p in patchers:
            self.addCleanup(p.stop)
        for p in patchers:
            p.start()

    def test_partial_header_read(self):
        c = self.make_connection()

        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        message = self.make_msg(header, options)

        # read in the first byte
        c._socket.recv.return_value = message[0:1]
        c.handle_read(None, 0)
        self.assertEqual(c._iobuf.getvalue(), message[0:1])

        c._socket.recv.return_value = message[1:]
        c.handle_read(None, 0)
        self.assertEqual(six.binary_type(), c._iobuf.getvalue())

        # let it write out a StartupMessage
        c.handle_write(None, 0)

        header = self.make_header_prefix(ReadyMessage, stream_id=1)
        c._socket.recv.return_value = self.make_msg(header)
        c.handle_read(None, 0)

        self.assertTrue(c.connected_event.is_set())
        self.assertFalse(c.is_defunct)

    def test_partial_message_read(self):
        c = self.make_connection()

        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        message = self.make_msg(header, options)

        # read in the first nine bytes
        c._socket.recv.return_value = message[:9]
        c.handle_read(None, 0)
        self.assertEqual(c._iobuf.getvalue(), message[:9])

        # ... then read in the rest
        c._socket.recv.return_value = message[9:]
        c.handle_read(None, 0)
        self.assertEqual(six.binary_type(), c._iobuf.getvalue())

        # let it write out a StartupMessage
        c.handle_write(None, 0)

        header = self.make_header_prefix(ReadyMessage, stream_id=1)
        c._socket.recv.return_value = self.make_msg(header)
        c.handle_read(None, 0)

        self.assertTrue(c.connected_event.is_set())
        self.assertFalse(c.is_defunct)

    def test_watchers_are_finished(self):
        """
        Test for asserting that watchers are closed in LibevConnection

        This test simulates a process termination without calling cluster.shutdown(), which would trigger
        LibevConnection._libevloop._cleanup. It will check the watchers have been closed
        Finally it will restore the LibevConnection reactor so it doesn't affect
        the rest of the tests

        @since 3.10
        @jira_ticket PYTHON-747
        @expected_result the watchers are closed

        @test_category connection
        """
        with patch.object(LibevConnection._libevloop, "_thread"), \
             patch.object(LibevConnection._libevloop, "notify"):

            self.make_connection()

            # We have to make a copy because the connections shouldn't
            # be alive when we verify them
            live_connections = set(LibevConnection._libevloop._live_conns)

            # This simulates the process ending without cluster.shutdown()
            # being called, then with atexit _cleanup for libevreactor would
            # be called
            libev__cleanup(weakref.ref(LibevConnection._libevloop))
            for conn in live_connections:
                for watcher in (conn._write_watcher, conn._read_watcher):
                    self.assertTrue(watcher.stop.mock_calls)

        LibevConnection._libevloop._shutdown = False

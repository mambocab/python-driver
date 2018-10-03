try:
    from cassandra.io.asyncioreactor import AsyncioConnection
    import asynctest
    ASYNCIO_AVAILABLE = True
except (ImportError, SyntaxError):
    AsyncioConnection = None
    ASYNCIO_AVAILABLE = False

from tests import is_monkey_patched, connection_class
from tests.unit.io.utils import TimerCallback, TimerTestMixin, ReactorTestMixin

from mock import patch
import socket

import unittest
import time
skip_me = (is_monkey_patched() or
           (not ASYNCIO_AVAILABLE) or
           (connection_class is not AsyncioConnection))


import logging

log = logging.getLogger(__name__)


class AsyncioTestMixin(object):

    @classmethod
    def setUpClass(cls):
        if skip_me:
            return
        cls.connection_class = AsyncioConnection
        AsyncioConnection.initialize_reactor()

    @classmethod
    def tearDownClass(cls):
        if skip_me:
            return
        if ASYNCIO_AVAILABLE and AsyncioConnection._loop:
            AsyncioConnection._loop.stop()

    @property
    def create_timer(self):
        return self.connection.create_timer

    @property
    def _timers(self):
        raise RuntimeError('no TimerManager for AsyncioConnection')

    def setUp(self):
        if skip_me:
            return
        socket_patcher = patch('socket.socket', spec=socket.socket)
        self.addCleanup(socket_patcher.stop)
        socket_patcher.start()

        old_selector = AsyncioConnection._loop._selector
        AsyncioConnection._loop._selector = asynctest.TestSelector()

        def reset_selector():
            AsyncioConnection._loop._selector = old_selector

        self.addCleanup(reset_selector)

        super(AsyncioTestMixin, self).setUp()


@unittest.skipIf(is_monkey_patched(), 'runtime is monkey patched for another reactor')
@unittest.skipIf(connection_class is not AsyncioConnection,
                 'not running asyncio tests; current connection_class is {}'.format(connection_class))
@unittest.skipUnless(ASYNCIO_AVAILABLE, "asyncio is not available for this runtime")
class AsyncioTimerTests(AsyncioTestMixin, TimerTestMixin, unittest.TestCase):

    def test_timer_cancellation(self):
        # Various lists for tracking callback stage
        timeout = .1
        callback = TimerCallback(timeout)
        timer = self.create_timer(timeout, callback.invoke)
        timer.cancel()
        # Release context allow for timer thread to run.
        time.sleep(.2)
        # Assert that the cancellation was honored
        self.assertFalse(callback.was_invoked())


@unittest.skipIf(is_monkey_patched(), 'runtime is monkey patched for another reactor')
@unittest.skipIf(connection_class is not AsyncioConnection,
                 'not running asyncio tests; current connection_class is {}'.format(connection_class))
@unittest.skipUnless(ASYNCIO_AVAILABLE, "asyncio is not available for this runtime")
class AsyncioConnectionTest(ReactorTestMixin, AsyncioTestMixin, unittest.TestCase):

    def make_connection(self):
        c = super(AsyncioConnectionTest, self).make_connection()

        import asyncio

        _cached_handle_write = c.handle_write
        _cached_handle_read = c.handle_read
        print(_cached_handle_read)
        assert asyncio.iscoroutinefunction(_cached_handle_read)
        print(_cached_handle_write)
        assert asyncio.iscoroutinefunction(_cached_handle_write)

        def handle_write_synchronous():
            log.debug('in handle_write_synchronous')
            rv = c._loop.call_soon_threadsafe(
                _cached_handle_write
            )
            return rv

        def handle_read_synchronous(*args, **kwargs):
            log.debug('in handle_read_synchronous')
            return c._loop.call_soon_threadsafe(
                _cached_handle_read
            )

        c.handle_write = handle_write_synchronous
        c.handle_read = handle_read_synchronous
        return c

    # internally, AsyncioConnection's handle_write blocks on having something
    # pushed to its queue, so we can't check that that writing calls `send` the
    # way that other reactors do
    test_blocking_on_write = unittest.skip('cannot test blocking on write')(
        ReactorTestMixin.test_blocking_on_write
    )

    socket_attr_name = '_socket'

from cassandra.io.asyncioreactor import AsyncioConnection
from tests import is_monkey_patched
from tests.unit.io.utils import ReactorTestMixin, TimerCallback, TimerTestMixin

from mock import patch

import unittest
import time

import asyncio
import mock
# import collections
# import weakref
import socket


import logging

log = logging.getLogger(__name__)

# stolen from python stdlib tests
def mock_nonblocking_socket(proto=socket.IPPROTO_TCP, type=socket.SOCK_STREAM,
                            family=socket.AF_INET):
    """Create a mock of a non-blocking socket."""
    sock = mock.MagicMock(socket.socket,
                          gettimeout=mock.Mock(),
                          settimeout=mock.Mock(),
                          connect=mock.Mock(),
                          setblocking=mock.Mock())
    sock.proto = proto
    sock.type = type
    sock.family = family
    sock.gettimeout.return_value = 0.0
    return sock


# also stolen from python stdlib tests
class TestSelector(asyncio.selectors.BaseSelector):

    def __init__(self):
        self.keys = {}

    def register(self, fileobj, events, data=None):
        key = asyncio.selectors.SelectorKey(fileobj, 0, events, data)
        self.keys[fileobj] = key
        return key

    def unregister(self, fileobj):
        return self.keys.pop(fileobj)

    def select(self, timeout):
        return []

    def get_map(self):
        return self.keys


class AsyncioTimerTests(TimerTestMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if is_monkey_patched():
            return
        cls.connection_class = AsyncioConnection
        AsyncioConnection.initialize_reactor()

    @property
    def create_timer(self):
        return self.connection.create_timer

    @property
    def _timers(self):
        raise RuntimeError('no TimerManager for AsyncioConnection')

    def setUp(self):
        # socket_patcher = patch('socket.socket')
        # self.addCleanup(socket_patcher.stop)
        # socket_patcher.start()

        super(AsyncioTimerTests, self).setUp()

    # parent's test_timer_cancellation depends on the connection class having a
    # timer manager; AsyncioConnection doesn't
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


class TestBaseSelectorEventLoop(asyncio.selector_events.BaseSelectorEventLoop):

    __test__ = False

    def _make_self_pipe(self):
        self._ssock = mock.Mock()
        self._csock = mock.Mock()
        self._internal_fds += 1

    def _close_self_pipe(self):
        pass


class AsyncioUtilMixin(object):
    def set_event_loop(self, loop, cleanup=True):
        assert loop is not None
        # ensure that the event loop is passed explicitly in asyncio
        # asyncio.events.set_event_loop(None)
        asyncio.events.set_event_loop(None)
        if cleanup:
            self.addCleanup(self.close_loop, loop)

    @staticmethod
    def close_loop(loop):
        executor = loop._default_executor
        if executor is not None:
            executor.shutdown(wait=True)
        loop.close()


class AsyncioReactorTest(ReactorTestMixin, AsyncioUtilMixin, unittest.TestCase):
    connection_class = AsyncioConnection
    socket_attr_name = '_socket'
    patchers = ()

    def make_connection(self):
        c = super(AsyncioReactorTest, self).make_connection()

        # mixin tests assume we can call to run it once, so we stop the
        # background processes that run them continuously
        c._read_watcher.cancel()
        c._write_watcher.cancel()
        # patch watchers so .close won't choke trying too cancel them again
        c._write_watcher = mock.Mock()
        c._read_watcher = mock.Mock()

        # without this, just sending and processing connection messages doesn't
        # set the event, which might itself be worth looking into
        c.connected_event.set()

        return c

    @classmethod
    def setUpClass(cls):
        cls.patchers = (
            patch('socket.socket', new=mock_nonblocking_socket()),
        )
        for p in cls.patchers:
            p.start()

    @classmethod
    def tearDownClass(cls):
        for p in cls.patchers:
            p.stop()

    def setUp(self):
        self.selector = mock.Mock()
        self.selector.select.return_value = []
        self.loop = TestBaseSelectorEventLoop(self.selector)

        #
        self.loop._add_reader = mock.Mock()
        self.loop.add_reader = mock.Mock()
        self.loop._add_reader._is_coroutine = False
        self.loop._add_writer = mock.Mock()
        self.loop.add_writer = mock.Mock()
        self.loop._remove_reader = mock.Mock()
        self.loop._remove_writer = mock.Mock()
        #

        self.set_event_loop(self.loop)

        AsyncioConnection.initialize_reactor(self.loop)

        super(AsyncioReactorTest, self).setUp()

        # AsyncioConnection._loop._selector.select.return_value = []

    def tearDown(self):
        self.loop.stop()
        AsyncioConnection._loop_thread.join()

    def get_handle_read(self, connection):
        def handle_read_synchronous():
            # this submits the task to the loop, but doesn't wait for it to
            # complete. blocking with result()
            asyncio.run_coroutine_threadsafe(
                coro=connection.handle_read(),
                loop=self.connection_class._loop
            )

            # log.debug('handle_read: calling _run_once')
            # self.connection_class._loop._run_once()

        return handle_read_synchronous

    def get_handle_write(self, connection):
        def handle_write_synchronous():
            # print(connection)
            # print(connection.handle_write)
            asyncio.run_coroutine_threadsafe(
                coro=connection.handle_write(),
                loop=self.connection_class._loop
            )

            # log.debug('handle_read: calling _run_once')
            # self.connection_class._loop._run_once()

        return handle_write_synchronous

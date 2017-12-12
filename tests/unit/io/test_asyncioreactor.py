from cassandra.io.asyncioreactor import AsyncioConnection
from tests import is_monkey_patched
from tests.unit.io.utils import ReactorTestMixin, TimerCallback, TimerTestMixin

from mock import patch

import unittest
import time

import asyncio
import mock
import collections
import weakref
import socket

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


class TestLoop(asyncio.base_events.BaseEventLoop):
    """Loop for unittests.

    It manages self time directly.
    If something scheduled to be executed later then
    on next loop iteration after all ready handlers done
    generator passed to __init__ is calling.

    Generator should be like this:

        def gen():
            ...
            when = yield ...
            ... = yield time_advance

    Value returned by yield is absolute time of next scheduled handler.
    Value passed to yield is time advance to move loop's time forward.
    """

    def __init__(self, gen=None):
        super().__init__()

        if gen is None:
            def gen():
                yield
            self._check_on_close = False
        else:
            self._check_on_close = True

        self._gen = gen()
        next(self._gen)
        self._time = 0
        self._clock_resolution = 1e-9
        self._timers = []
        self._selector = TestSelector()

        self.readers = {}
        self.writers = {}
        self.reset_counters()

        self._transports = weakref.WeakValueDictionary()

    def time(self):
        return self._time

    def advance_time(self, advance):
        """Move test time forward."""
        if advance:
            self._time += advance

    def close(self):
        super().close()
        if self._check_on_close:
            try:
                self._gen.send(0)
            except StopIteration:
                pass
            else:  # pragma: no cover
                raise AssertionError("Time generator is not finished")

    def _add_reader(self, fd, callback, *args):
        self.readers[fd] = asyncio.events.Handle(callback, args, self)

    def _remove_reader(self, fd):
        self.remove_reader_count[fd] += 1
        if fd in self.readers:
            del self.readers[fd]
            return True
        else:
            return False

    def assert_reader(self, fd, callback, *args):
        assert fd in self.readers, 'fd {} is not registered'.format(fd)
        handle = self.readers[fd]
        assert handle._callback == callback, '{!r} != {!r}'.format(
            handle._callback, callback)
        assert handle._args == args, '{!r} != {!r}'.format(
            handle._args, args)

    def _add_writer(self, fd, callback, *args):
        self.writers[fd] = asyncio.events.Handle(callback, args, self)

    def _remove_writer(self, fd):
        self.remove_writer_count[fd] += 1
        if fd in self.writers:
            del self.writers[fd]
            return True
        else:
            return False

    def assert_writer(self, fd, callback, *args):
        assert fd in self.writers, 'fd {} is not registered'.format(fd)
        handle = self.writers[fd]
        assert handle._callback == callback, '{!r} != {!r}'.format(
            handle._callback, callback)
        assert handle._args == args, '{!r} != {!r}'.format(
            handle._args, args)

    def _ensure_fd_no_transport(self, fd):
        try:
            transport = self._transports[fd]
        except KeyError:
            pass
        else:
            raise RuntimeError(
                'File descriptor {!r} is used by transport {!r}'.format(
                    fd, transport))

    def add_reader(self, fd, callback, *args):
        """Add a reader callback."""
        self._ensure_fd_no_transport(fd)
        return self._add_reader(fd, callback, *args)

    def remove_reader(self, fd):
        """Remove a reader callback."""
        self._ensure_fd_no_transport(fd)
        return self._remove_reader(fd)

    def add_writer(self, fd, callback, *args):
        """Add a writer callback.."""
        self._ensure_fd_no_transport(fd)
        return self._add_writer(fd, callback, *args)

    def remove_writer(self, fd):
        """Remove a writer callback."""
        self._ensure_fd_no_transport(fd)
        return self._remove_writer(fd)

    def reset_counters(self):
        self.remove_reader_count = collections.defaultdict(int)
        self.remove_writer_count = collections.defaultdict(int)

    def _run_once(self):
        super()._run_once()
        for when in self._timers:
            advance = self._gen.send(when)
            self.advance_time(advance)
        self._timers = []

    def call_at(self, when, callback, *args):
        self._timers.append(when)
        return super().call_at(when, callback, *args)

    def _process_events(self, event_list):
        return

    def _write_to_self(self):
        pass


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


class AsyncioReactorTest(ReactorTestMixin, unittest.TestCase):
    connection_class = AsyncioConnection
    socket_attr_name = '_socket'
    patchers = ()

    def set_event_loop(self, loop, cleanup=True):
        assert loop is not None
        # ensure that the event loop is passed explicitly in asyncio
        # asyncio.events.set_event_loop(None)
        asyncio.events.set_event_loop(None)
        if cleanup:
            self.addCleanup(self.close_loop, loop)

    def new_test_loop(self, gen=None):
        loop = TestLoop(gen)
        self.set_event_loop(loop)

        #
        # loop._add_reader = mock.Mock()
        # loop._add_reader._is_coroutine = False
        # loop._add_writer = mock.Mock()
        # loop._remove_reader = mock.Mock()
        # loop._remove_writer = mock.Mock()
        #

        return loop
    new_test_loop.__test__ = False

    @staticmethod
    def close_loop(loop):
        executor = loop._default_executor
        if executor is not None:
            executor.shutdown(wait=True)
        loop.close()

    @classmethod
    def setUpClass(cls):
        # AsyncioConnection.initialize_reactor()
        cls.patchers = (
            patch('socket.socket', new=mock_nonblocking_socket()),
        )
        for p in cls.patchers:
            p.start()

        # AsyncioConnection._loop._selector.select.return_value = []

    @classmethod
    def tearDownClass(cls):
        for p in cls.patchers:
            p.stop()

    def setUp(self):
        self.selector = mock.Mock()
        self.selector.select.return_value = []
        self.loop = TestBaseSelectorEventLoop(self.selector)
        self.set_event_loop(self.loop)

        AsyncioConnection.initialize_reactor(self.loop)

        super(AsyncioReactorTest, self).setUp()

    def tearDown(self):
        self.loop.stop()
        AsyncioConnection._loop_thread.join()

    def get_handle_read(self, connection):
        def handle_read_synchronous():
            print(connection)
            print(connection.handle_read)
            asyncio.run_coroutine_threadsafe(
                coro=connection.handle_read(),
                loop=self.connection_class._loop
            )
            self.connection_class._loop._run_once()
            # self.connection_class._loop.call_soon(self.connection_class._loop.stop)
            # self.connection_class._loop.run_forever()

        return handle_read_synchronous

    def get_handle_write(self, connection):
        def handle_write_synchronous():
            print(connection)
            print(connection.handle_write)
            asyncio.run_coroutine_threadsafe(
                coro=connection.handle_write(),
                loop=self.connection_class._loop
            )
            self.connection_class._loop._run_once()
            # self.connection_class._loop.call_soon(self.connection_class._loop.stop)
            # self.connection_class._loop.run_forever()

        return handle_write_synchronous

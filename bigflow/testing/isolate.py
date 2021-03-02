"""Mixins to isolate `unittest.TestCase` into separate processes

May prevent from doing side effects with Python intepreter
(loaded modules, global variables, caches etc).
"""

# `*IsolateMixin`s overrides original `unittest.TestCase.run` method.
# It spawns a new subprocess via `multiprocessing`.
# With 'spawn' strategy the whole unittest is pickled and passend into subprocess.
# With 'fork' strategy pickling is not involved.
#
# Then parent creates proxy to instance of `unittest.TestResult` and pass proxy to
# child process.  Such synchronously sends information about all methods calls
# to parent via duplex pipe.  Parent recieves information and mutate
# original 'TestResult' appropriately.
#
# You can find sequence diagramm for this in "isolate_sequence.puml" file.


import unittest
import sys
import time
import contextlib
import multiprocessing
import threading


from bigflow.commons import public


class _ObjectTag:
    """Stub for non-pickable objects, which needed to be transferred between processes"""

    def __init__(self, tag):
        self.tag = tag

    @classmethod
    def forfeit(cls, obj, obj2tag):
        return cls(obj2tag[obj]) if obj in obj2tag else obj

    @classmethod
    def recover(cls, obj, tag2obj):
        return tag2obj[obj.tag] if isinstance(obj, cls) else obj


class _TestResultProxy:
    """Dump proxy for `TestResult`. Stream all public method calls into channel."""

    def __init__(self, ch, testcase_proxy, tblib):
        self._ch = ch
        self._tc_proxy = testcase_proxy
        self._tblib = tblib
        self._lock = threading.Lock()

    # proxy all public method calls
    def __getattr__(self, name: str):
        assert not name.startswith("_")

        def method(*args):
            with self._lock:
                # replace ref to proxy testcase by sepulka
                smap = {self._tc_proxy: 'testcase'}
                args = [_ObjectTag.forfeit(a, smap) for a in args]

                if isinstance(args[-1], tuple) and not self._tblib:
                    # last arg is 'exc_info', but `tblib` is not installed
                    args[-1] = (None, None, None)

                self._ch.send((name, tuple(args)))
                return self._ch.recv()

        return method


class _IsolatedProcessMixin(unittest.TestCase):

    timeout: float
    isolate: bool

    _was_executed: bool = False
    _isolated_test_spawn_method: str

    def _enable_tblib(self):
        try:
            import tblib.pickling_support
        except ImportError:
            print("Failed to import `tblib`, tracebask/stacktracs might be stripped")
            return False
        else:
            tblib.pickling_support.install()
            return True

    def _stop_process(self, p):
        if p.is_alive():
            p.kill()
            p.join(timeout=30)
        p.close()

    def _fail_into_result(self, result: unittest.TestResult, msg):
        """Include error with the traceback into `TestResult`"""
        try:
            raise RuntimeError(msg)
        except RuntimeError:
            result.addFailure(self, sys.exc_info())

    def run0(self, ch, tblib_enabled):
        # executed from child process
        tblib_enabled = tblib_enabled and self._enable_tblib()
        with ch:
            super().run(_TestResultProxy(ch, self, tblib_enabled))
            ch.send(('__stop__', None))

    def _recv_testresult_proxy(self, result, process, ch, run_until):
        """Recieve and applly method calls on real TestResult object"""

        poll_delay = 0.001
        while process.is_alive() and time.time() < run_until:
            if not ch.poll(poll_delay):
                poll_delay = min(1, poll_delay * 1.5)
                continue

            poll_delay = 0.001
            method, args = ch.recv()
            if method == '__stop__':
                return  # ok

            # recover ref to current testcase
            smap = {'testcase': self}
            args = tuple(_ObjectTag.recover(a, smap) for a in args)

            resp = getattr(result, method)(*args)
            ch.send(resp)

        if process.is_alive():
            self._fail_into_result(result, "Timeout: isolated test subprocess is stil running")
        else:
            self._fail_into_result(result, "Isotalted process was unexpectedly terminated")

    def run(self, result=None):
        cls = type(self)

        if not self.isolate:
            assert not cls._was_executed, "Test was already executed without isolation, either enable isolation or run single test at once"
            try:
                self.setUpParent()
                return super().run(result)
            finally:
                self.tearDownParent()

        cls._was_executed = True
        tblib_enabled = self._enable_tblib()

        if result is None:
            result = self.defaultTestResult()
        mp_context = multiprocessing.get_context(
            self._isolated_test_spawn_method)

        self.setUpParent()
        with contextlib.ExitStack() as cstack:
            cstack.callback(self.tearDownParent)

            server_ch, client_ch = mp_context.Pipe()
            cstack.callback(server_ch.close)
            cstack.callback(client_ch.close)

            process = mp_context.Process(
                name=f"isolated-test--{self.id}",
                target=self.run0, args=[client_ch, tblib_enabled],
            )
            process.start()
            cstack.callback(self._stop_process, process)

            run_until = time.time() + self.timeout
            self._recv_testresult_proxy(result, process, server_ch, run_until)
            process.join(max(1, run_until - time.time()))

    @property
    def isolate(self):
        # isolate only when running without debugger
        return sys.gettrace() == None

    def setUpParent(self):
        "Hook method for setting up the test fixture before exercising it. Runs in original python process."
        pass

    def tearDownParent(self):
        "Hook method for deconstructing the test fixture after testing it. Runs in original python process."
        pass


@public()
class ForkIsolateMixin(_IsolatedProcessMixin):
    """Mixin class for subclasses of `unittest.TestCase`

    Run all test methods in separate forked processes.
    All changes in variables/modules/memory made during test execution does not affect original Python instance.
    Note, that IO operations are not isolated, so external resources still need to be cleaned manually.

    Methods `setUpClass` and `tearDownClass` are executed in scope of main process.

    Methods `setUp` and `tearDown` are executed inside spawned process.
    Additional methods `setUpParent` and `tearDownParent` provided to run
    per-test fixtures in scope of parent process.
    """

    timeout: float = 3600
    isolate: bool

    _isolated_test_spawn_method = 'fork'


@public()
class SpawnIsolateMixin(_IsolatedProcessMixin):
    """Mixin class for subclasses of `unittest.TestCase`

    Run all test methods in fresh Python processes.
    Each process is inializated 'from scratch', which result into recreating/reloading all modules/classes.
    Note, that IO operations are not isolated, so external resources still need to be cleaned manually.

    Methods `setUpClass` and `tearDownClass` are executed in scope of main process,
    however it is not recommended to use them with this mixin.

    Methods `setUp` and `tearDown` are executed inside spawned process.
    Additional methods `setUpParent` and `tearDownParent` provided to run
    per-test fixtures in scope of parent process.

    Test instance and test class must be pickleable.
    """

    timeout: float = 3600
    isolate: bool

    _isolated_test_spawn_method = 'spawn'

    def __init__(self, *args, **kwargs):
        self._class_dict_orig = dict(self.__class__.__dict__)
        super().__init__(*args, **kwargs)

    # Pickle not only test instance state, but also state of test class
    # Only fields changed after test `__init__` are tranferred into subprocess.
    def __getstate__(self):
        selfdict = dict(self.__dict__)
        clsdict_orig = selfdict.pop('_class_dict_orig')
        clsdict = {
            k: v
            for k, v in self.__class__.__dict__.items()
            if k not in clsdict_orig or clsdict_orig[k] is not v
        }
        return selfdict, clsdict

    def __setstate__(self, state):

        selfdict, clsdict = state
        self.__dict__ = selfdict

        # dynamically overwrite test class with new state
        pcls = type(self.__class__.__name__, (self.__class__,), clsdict)
        self.__class__ = pcls

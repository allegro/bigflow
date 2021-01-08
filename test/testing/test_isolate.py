import os
import unittest

import tblib  # ensure installed

import bigflow.testing.isolate

from . import nonpure


class TestIsolateMixinTestCase(unittest.TestCase):

    class SubTest(unittest.TestCase):
        failed = False

        @classmethod
        def setUpClass(cls):
            cls.pid = os.getpid()

        def test_fail(self):
            type(self).failed = True
            self.fail('fail-ok')

        def test_ok(self):
            self.assertNotEqual(self.pid, os.getpid())

        def test_error(self):
            raise RuntimeError('fail-error')

        @unittest.skip('test-skip')
        def test_skip(self):
            self.fail()

        @unittest.expectedFailure
        def test_expected_failure_but(self):
            pass

        @unittest.expectedFailure
        def test_expected_failure(self):
            self.fail("expected")

    class ForkTest(bigflow.testing.ForkIsolateMixin, SubTest):
        pass

    class SpawnTest(bigflow.testing.SpawnIsolateMixin, SubTest):
        pass

    def check_test_mixin_subtest(self, test_class):
        # when
        result = self._run_test(test_class)

        # then
        self.assertEqual(6, result.testsRun, "All tests should run")

        # then
        self.assertEqual(1, len(result.failures))
        self.assertRegex(result.failures[0][0].id(), r".*\.test_fail", "Test 'test_fail' sould fail")
        self.assertIsNotNone(result.failures[0][1], "Traceback is attached")
        self.assertFalse(test_class.failed, "State is not propogated")

        # then]
        self.assertEqual(1, len(result.errors))
        self.assertRegex(result.errors[0][0].id(), ".*\.test_error", "Test 'test_error' sould raise exception")
        self.assertIsNotNone(result.errors[0][1], "Traceback is attached")

        # then
        self.assertEqual(1, len(result.skipped), "Single skipped test")
        self.assertEqual(1, len(result.unexpectedSuccesses), "Single unexpected success")
        self.assertEqual(1, len(result.expectedFailures), "Single expected failure")

    def _run_test(self, test_class):
        devnull = open(os.devnull, 'wt')
        self.addCleanup(devnull.close)
        runner = unittest.TextTestRunner(stream=devnull)
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(test_class)
        return runner.run(suite)

    @unittest.skip
    def test_fork_mixin(self):
        self.check_test_mixin_subtest(self.ForkTest)

    def test_spawn_mixin(self):
        self.check_test_mixin_subtest(self.SpawnTest)


class ForkReloadModulesTest(
    bigflow.testing.ForkIsolateMixin,
    unittest.TestCase,
):
    def setUpParent(self):
        super().setUpParent()
        self.const = nonpure.CONST

    def test_test(self):
        from . import nonpure
        self.assertEqual(self.const, nonpure.CONST, "Module should not be reloaded")


class SpawnReloadModulesTest(
    bigflow.testing.SpawnIsolateMixin,
    unittest.TestCase,
):
    def setUpParent(self):
        super().setUpParent()
        self.const = nonpure.CONST

    def test_test(self):
        from . import nonpure
        self.assertNotEqual(self.const, nonpure.CONST, "Module should be reloaded")


class _IsolateDisabledBase(
    bigflow.testing.isolate._IsolatedProcessMixin,
    unittest.TestCase,
):
    isolate = False
    _state = None

    @classmethod
    def _change_state(cls, from_, to_):
        if cls._state != from_:
            raise AssertionError(f"invalid state, expected {from_} but got {cls._state}")
        cls._state = to_

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._change_state(None, 'setupClass')

    def setUpParent(self):
        super().setUpParent()
        self._change_state('setupClass', 'setupParent')

    def setUp(self):
        super().setUp()
        self._change_state('setupParent', 'setup')

    def test_test(self):
        #self.assertEqual(self.thepid, os.getpid())
        self._change_state('setup', 'run')

    def tearDown(self):
        self._change_state('run', 'teardown')
        super().tearDown()

    def tearDownParent(self):
        self._change_state('teardown', 'teardownParent')
        super().tearDownParent()

    @classmethod
    def tearDownClass(cls):
        cls._change_state('teardownParent', None)
        super().tearDownClass()


class ForkDisabledTest(
    _IsolateDisabledBase,
    bigflow.testing.ForkIsolateMixin,
    unittest.TestCase,
):
    pass


class SpawnDisabledTest(
    _IsolateDisabledBase,
    bigflow.testing.SpawnIsolateMixin,
    unittest.TestCase,
):
    pass
import os
import contextlib
import unittest
import unittest.mock

import pickle
import dill

from bigflow.konfig import Konfig, resolve_konfig, dynamic, expand, fromenv


class TestKonfig(unittest.TestCase):

    def setUp(self) -> None:

        self.mocks = contextlib.ExitStack()
        self.addCleanup(self.mocks.close)

        # global mocks
        self.mocks.enter_context(unittest.mock.patch.dict('os.environ'))


    def test_simple_konfig(self):

        # given
        class C(Konfig):
            a = 1
            b = '2'

        # when
        config = C()

        # then
        self.assertEqual(config.a, 1)
        self.assertEqual(config.b, '2')
        self.assertEqual(dict(config), dict(a=1, b='2'))

    def test_override_props(self):

        # given
        class C(Konfig):
            a = 1
            b = '2'

        # when
        config = C(a=100, c=3)

        # then
        self.assertEqual(config.a, 100)
        self.assertEqual(config.b, '2')
        self.assertEqual(config.c, 3)

    def test_noclass_konfig(self):

        # when
        config = Konfig(a=1, b='2')

        # then
        self.assertEqual(config.a, 1)
        self.assertEqual(config.b, '2')
        self.assertEqual(dict(config), {'a': 1, 'b': '2'})

    def test_simple_config_named(self):

        # given
        class C(Konfig, name="the-config"):
            a = 1

        # when
        config = C()

        # then
        self.assertEqual(config.a, 1)
        self.assertEqual(config.name, "the-config")
        self.assertEqual(dict(config), dict(a=1))

    def test_inheritance(self):

        # given
        class A(Konfig):
            x = "a"
            y = "a"

        class B(A):
            x = "b"

        class C(B):
            y = "config"

        class D(C):
            pass

        # when
        a = A()
        b = B()
        c = C()
        d = D()

        # then
        self.assertEqual(dict(a), dict(x="a", y="a"))
        self.assertEqual(dict(b), dict(x="b", y="a"))
        self.assertEqual(dict(c), dict(x="b", y="config"))
        self.assertEqual(dict(d), dict(x="b", y="config"))

    def test_inheritance_diamond(self):

        # given
        class A(Konfig):
            x = "A"
            y = "A"
            z = "A"

        class B(A):
            x = "B"
            z = "?"

        class C(A):
            y = "C"

        class D(C, B):
            z = "D"

        # when
        config = D()

        # then
        self.assertEqual(dict(config), dict(x="B", y="C", z="D"))

    def test_osenv_variables(self):
        # given
        os.environ.update(
            bf_b='x',
            bf_c='x',
            # bf_d - miss
            bf_e='1',
        )

        class C(Konfig):
            b = fromenv('b')
            c = fromenv('c', default="none")
            d = fromenv('d', default="none")
            e = fromenv('e', type=int)

        # when
        config = C()

        # then
        self.assertEqual(config.b, 'x')
        self.assertEqual(config.c, 'x')
        self.assertEqual(config.d, "none")
        self.assertEqual(config.e, 1)

    def test_osenv_missing(self):
        # given
        class C(Konfig):
            x = fromenv("missing")

        # expect
        with self.assertRaises(ValueError):
            C()

    def test_placeholder_expansion(self):
        # given
        class C(Konfig, name="A"):
            a = expand("{e}")
            b = expand("pre{a}post")
            c = expand("{a}-{a}")
            d = expand("pre{{a}}post")
            e = expand("{name}")

        # when
        config = C()

        # then
        self.assertEqual(
            dict(config),
            dict(
                a="A",
                b="preApost",
                c="A-A",
                d="pre{a}post",
                e="A",
            ))

    def test_placeholder_expansion_inheritance(self):

        # given
        class A(Konfig, name="A"):
            a = "0"
            b = "2"
            c = "3"
            x = expand("{a}{b}{c}")
            y = expand("{x}{x}")
            z = expand("{y}{name}")
            k = expand("{z}")

        class B(A, name="B"):
            a = "1"

        class C(B, name="C"):
            z = expand("{a}/{b}/{name}")

        # when
        config = C()

        # then
        self.assertEqual(
            dict(config),
            dict(
                a="1",
                b="2",
                c="3",
                x="123",
                y="123123",
                z="1/2/C",
                k="1/2/C",
            ))

    def test_resolve_configs(self):

        # given
        class A(Konfig, name="a"): pass
        class B(A, name="b"): pass
        class C(B, name="c"): pass

        # expect
        self.assertIs(A.resolve("a").__class__, A)
        self.assertIs(A.resolve("b").__class__, B)
        self.assertIs(A.resolve("c").__class__, C)

    def test_resolve_configs_fail(self):
        # given
        class A(Konfig): pass
        class B(A, name="B"): pass

        # expect
        with self.assertRaises(ValueError):
            A.resolve('A').name
        with self.assertRaises(ValueError):
            A.resolve('C').name

    def test_resolve_config_by_osenv(self):
        # given
        os.environ['bf_env'] = 'dev'

        class C(Konfig): pass
        class _(C, name="prod"): bb = 1
        class _(C, name="dev"): bb = 2

        # when
        config = C.resolve()

        # then
        self.assertEqual(config.bb, 2)
        self.assertEqual(config.name, 'dev')

    def test_resolve_config_to_default(self):
        # given
        class C(Konfig): pass
        class _(C, name="prod"): bb = 1
        class _(C, name="dev"): bb = 2

        # when
        config = C.resolve(default='prod')

        # then
        self.assertEqual(config.bb, 1)
        self.assertEqual(config.name, 'prod')

    class serialization_A(Konfig):
        a = "1"
        b = "2"
        x = fromenv('x')

    class serialization_B(serialization_A):
            a = "3"
            y = expand("{x}")

    def test_serialization(self):
        # given
        os.environ['bf_x'] = "1"
        config = self.serialization_B(z="ok")

        # when
        s = pickle.dumps(config)
        config2 = pickle.loads(s)

        s = dill.dumps(config)
        config3 = dill.loads(s)

        # then
        self.assertEqual(dict(config), dict(config2))
        self.assertEqual(dict(config), dict(config3))


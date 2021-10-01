import os
import contextlib
import unittest
import unittest.mock
import pickle

import dill  # type: ignore

from bigflow.konfig import Konfig, dynamic_super, merge, resolve_konfig, dynamic, expand, fromenv


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
            b='x',
            c='x',
            # d - miss
            e='1',
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
        class C(Konfig):
            a = expand("{e}")
            b = expand("pre{a}post")
            c = expand("{a}-{a}")
            d = expand("pre{{a}}post")
            e = expand("A")

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
        class A(Konfig):
            n = "A"
            a = "0"
            b = "2"
            c = "3"
            x = expand("{a}{b}{c}")
            y = expand("{x}{x}")
            z = expand("{y}")
            k = expand("{z}")

        class B(A):
            n = "B"
            a = "1"

        class C(B):
            n = "C"
            z = expand("{a}/{b}/{n}")

        # when
        config = C()

        # then
        self.assertEqual(
            dict(config),
            dict(
                n="C",
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
        class A(Konfig): pass
        class B(A): pass
        class C(B): pass
        cm = {'a': A, 'b': B, 'c': C}

        # expect
        self.assertIs(resolve_konfig(cm, "a").__class__, A)
        self.assertIs(resolve_konfig(cm, "b").__class__, B)
        self.assertIs(resolve_konfig(cm, "c").__class__, C)

    def test_resolve_configs_fail(self):
        # given
        class A(Konfig): pass
        class B(A): pass

        # expect
        with self.assertRaises(ValueError):
            resolve_konfig({'b': B}, "A", lazy=False)

    def test_resolve_config_by_osenv(self):
        # given
        os.environ['bf_env'] = 'dev'

        class C(Konfig): pass
        class P(C): bb = 1
        class D(C): bb = 2

        # when
        config = resolve_konfig({'prod': P, 'dev': D})

        # then
        self.assertEqual(config.bb, 2)

    def test_resolve_config_to_default(self):
        # given
        class C(Konfig):
            pass
        class A(C):
            bb = 1
        class B(C):
            bb = 2

        # when
        config = resolve_konfig(dict(prod=A, dev=B), default='prod')

        # then
        self.assertEqual(config.bb, 1)

    def test_resolve_returns_lazy(self):
        # given
        getit = unittest.mock.Mock()

        class A(Konfig):
            x = dynamic(getit)

        # when
        a = resolve_konfig({'a': A}, 'a')

        # then
        getit.assert_not_called()
        getit.return_value = 123

        self.assertEqual(a.x, 123)
        getit.assert_called_once_with(a)

    class serialization_A(Konfig):
        a = "1"
        b = "2"
        x = fromenv('x')

    class serialization_B(serialization_A):
            a = "3"
            y = expand("{x}")

    def test_serialization(self):
        # given
        os.environ['x'] = "1"
        config = self.serialization_B(z="ok")

        # when
        s = pickle.dumps(config)
        config2 = pickle.loads(s)

        s = dill.dumps(config)
        config3 = dill.loads(s)

        # then
        self.assertEqual(dict(config), dict(config2))
        self.assertEqual(dict(config), dict(config3))

    def test_replace(self):

        # given
        class C(Konfig):
            a = 1
            b = '2'

        # when
        config = C().replace(a=123)

        # then
        self.assertEqual(config.a, 123)
        self.assertEqual(config.b, '2')

    def test_dynamic_super(self):

        # given
        class A(Konfig):
            x = 1
            y = 1

        class B(A):
            x = dynamic_super(lambda s, x: x + s.y)
            y = 2

        class C(A):
            y = 3

        class D(C, B):
            x = dynamic_super(lambda s, x: x * s.y)

        # when
        a = A()
        b = B()
        c = C()
        d = D()

        # then
        self.assertEqual(dict(a), dict(x=1, y=1))
        self.assertEqual(dict(b), dict(x=3, y=2))
        self.assertEqual(dict(c), dict(x=1, y=3))

        # both lambas are executed with 's.y == 3'
        self.assertEqual(dict(d), dict(x=12, y=3))

    def test_merge(self):

        # given
        class A(Konfig):
            x = 1
            y = {
                'str1': "value",
                'str2': "value",
                'bool1': True,
                'bool2': True,
                'int1': 10,
                'int2': 10,
                'float1': 1.23,
                'float2': 1.23,
                'list1': [1, 2, 3],
                'list2': [1, 2, 3],
                'dict1': {'x': 2, 'y': 3},
                'dict2': {'x': 2, 'y': 3},
            }

        class B(A):
            x = merge((1).__add__)
            y = merge({
                'str2': "new-value",
                'bool2': False,
                'int2': 20,
                'float2': 3.21,
                'list2': [4].__add__,
                'dict2': {'x': (10).__mul__, 'y': 40},
            })

        # when
        a = A()
        b = B()

        # then
        self.assertEqual(dict(b), {
            'x': 2,
            'y': {
                'str1': "value",
                'str2': "new-value",
                'bool1': True,
                'bool2': False,
                'int1': 10,
                'int2': 20,
                'float1': 1.23,
                'float2': 3.21,
                'list1': [1, 2, 3],
                'list2': [4, 1, 2, 3],
                'dict1': {'x': 2, 'y': 3},
                'dict2': {'x': 20, 'y': 40},
            }
        })

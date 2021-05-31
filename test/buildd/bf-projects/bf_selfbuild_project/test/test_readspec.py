import unittest
import bigflow.build.reflect


class ReadProjectSpecTestCase(unittest.TestCase):
    def test_should_pass(self):
        spec = bigflow.build.reflect.get_project_spec()
        self.assertEqual(spec.name, "bf-selfbuild-project")


if __name__ == '__main__':
    unittest.main()

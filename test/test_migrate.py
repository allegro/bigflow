import subprocess
import time
from subprocess import PIPE

from test import mixins

class _BaseTestCase(
    mixins.BfCliInteractionMixin,
    mixins.PrototypedDirMixin,
):
    pass


class TestMigrateFromV10(_BaseTestCase):
    proto_dir = "bf-projects/bf_simple_v10"

    def test_automigrate_to_v11_two(self):
        """Launch 'bigflow project-version' command from 'v1.0' project."""

        # when
        p = self.bigflow_spawn(["project-version"], timeout=10)

        # then
        p.expect(r"project_setup.py.*renamed.*setup.py")
        p.expect(r"Do you wan to run migration now\?")
        p.sendline("YES")

        p.read()  # ignore the rest of output
        self.assertFalse(p.wait())

        self.assertFileNotExists("project_setup.py")
        self.assertFileExists("pyproject.toml")
        self.assertFileExists("setup.py")


class TestMigrateFromV11(_BaseTestCase):
    proto_dir = "bf-projects/bf_simple_v11"

    def test_dont_migrate_when_already_v11(self):
        # when
        cp = self.bigflow_run(["project-version"], check=False, capture_output=True)

        # then nothing should actually happen :)
        self.assertFalse(cp.returncode)

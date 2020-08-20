#!/usr/bin/python
# -*- coding: utf-8 -*-
import distutils.cmd
import setuptools
import subprocess
import os
from pathlib import Path

from bigflow.build import clear_package_leftovers

with open("README.md", "r") as fh:
    long_description = fh.read()

with open(os.path.join('requirements', 'base.txt'), 'r') as base_requirements:
    install_requires = [l.strip() for l in base_requirements.readlines()]

with open(os.path.join('requirements', 'stackdriver_extras.txt'), 'r') as stackdriver_extras_requirements:
    stackdriver_extras_require = [l.strip() for l in stackdriver_extras_requirements.readlines()]


class BuildAndInstallWheelCommand(distutils.cmd.Command):
    description = 'BigFlow build.'
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        clear_package_leftovers(
            Path(__file__).parent / 'dist',
            Path(__file__).parent / 'bigflow.egg-info',
            Path(__file__).parent / 'build')
        self.run_command('bdist_wheel')
        print(subprocess.getoutput('source env/bin/activate;pip install bigflow --find-links dist/'))


setuptools.setup(
    name="bigflow",
    version="1.0.dev3",
    author=u"Chi",
    author_email="chibox-team@allegrogroup.com",
    description="BigQuery client wrapper with clean API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/allegro/bigflow",
    packages=setuptools.find_packages(exclude=('test', 'e2e')),
    data_files=[
        ('requirements', ['requirements/base.txt', 'requirements/stackdriver_extras.txt']),
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=install_requires,
    extras_require={
        'stackdriver': stackdriver_extras_require
    },
    scripts=["bf"],
    cmdclass={
        'build_and_install_wheel': BuildAndInstallWheelCommand
    }
)

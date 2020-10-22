#!/usr/bin/python
# -*- coding: utf-8 -*-
import setuptools
import os


with open("README.md", "r") as fh:
    long_description = fh.read()


def read_requirements(name):
    with open(os.path.join("requirements", name)) as f:
        return list(map(str.strip, f))


with open(os.path.join('bigflow', '_version.py'), 'r') as version_file:
    version_globals = {}
    exec(version_file.read(), version_globals)
    __version__ = version_globals['__version__']


setuptools.setup(
    name="bigflow",
    version=__version__,
    author=u"Chi",
    author_email="chibox-team@allegrogroup.com",
    description="BigQuery client wrapper with clean API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/allegro/bigflow",
    packages=setuptools.find_packages(exclude=('test', 'e2e')),
    include_package_data=True,
    data_files=[
        ('requirements', [
            'requirements/base.txt',
            'requirements/monitoring_extras.txt',
            'requirements/bigquery_extras.txt',
            'requirements/log_extras.txt',
            'requirements/dataflow_extras.txt',
            'requirements/dataproc_extras.txt',
        ]),
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=read_requirements("base.txt"),
    extras_require={
        'monitoring': read_requirements("monitoring_extras.txt"),
        'bigquery': read_requirements("bigquery_extras.txt"),
        'log': read_requirements("log_extras.txt"),
        'dataproc': read_requirements("dataproc_extras.txt"),
        'dataflow': read_requirements("dataflow_extras.txt"),
    },
    scripts=["scripts/bf", "scripts/bigflow"],
)

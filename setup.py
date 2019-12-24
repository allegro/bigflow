#!/usr/bin/python
# -*- coding: utf-8 -*-
import setuptools
import os

with open("README.md", "r") as fh:
    long_description = fh.read()

with open(os.path.join('requirements', 'base.txt'), 'r') as base_requirements:
    install_requires = [l.strip() for l in base_requirements.readlines()]

with open(os.path.join('requirements', 'beam_extras.txt'), 'r') as beam_extras_requirements:
    beam_extras_require = [l.strip() for l in beam_extras_requirements.readlines()]

with open(os.path.join('requirements', 'stackdriver_extras.txt'), 'r') as stackdriver_extras_requirements:
    stackdriver_extras_require = [l.strip() for l in stackdriver_extras_requirements.readlines()]


setuptools.setup(
    name="biggerquery",
    version="0.6.dev7",
    author=u"Chi",
    author_email="chibox-team@allegrogroup.com",
    description="BigQuery client wrapper with clean API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/allegro/biggerquery",
    packages=setuptools.find_packages(exclude=('test', 'e2e')),
    data_files=[
        ('requirements', ['requirements/base.txt', 'requirements/beam_extras.txt', 'requirements/stackdriver_extras.txt']),
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=install_requires,
    extras_require={
        'beam': beam_extras_require,
        'stackdriver': stackdriver_extras_require
    },
)
#!/usr/bin/python
# -*- coding: utf-8 -*-
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="biggerquery",
    version="0.2.2",
    author=u"Chi",
    author_email="chibox-team@allegrogroup.com",
    description="BigQuery client wrapper with clean API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/allegro/biggerquery",
    packages=["biggerquery"],
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "google-cloud-bigquery>=1.12.0, <1.18.0",
        "pandas>=0.23.0, <0.24",
        "apache-beam[gcp,test]>=2.12, <=2.15",
        "google-cloud-core>=1.0.0, <=1.0.3",
        "numpy>=1.14.0, <1.17",
        "avro>=1.8.2, <=1.9.0"
    ]
)

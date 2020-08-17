# Docs project

## Installation

In the **docs project directory**, run:

`cd ..;python setup.py build_and_install_wheel`

That command builds the bigflow package and installs it on your environment.

## Usage

After installation, you can use BigFlow CLI to run a example from the documentation.

Examples:

`bf run --workflow simple_workflow`

`bf run --job simple_workflow.simple_job`

`bf run --job hourly_workflow.hourly_job --runtime '2020-08-01 12:00:00'`

`bf build`

`bf build --workflow simple_workflow --start-time '2020-01-01 00:00:00' --export-image-to-file` 
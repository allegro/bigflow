# Docs project

## Installation

[Install BigFlow](../README.md#installing-bigflow) in the **docs project directory**.

## Usage

After installation, you can use BigFlow CLI to run a example from the documentation.

Examples:

`bigflow run --workflow simple_workflow`

`bigflow run --job simple_workflow.simple_job`

`bigflow run --job hourly_workflow.hourly_job --runtime '2020-08-01 12:00:00'`

`bigflow build`

`bigflow build --workflow simple_workflow --start-time '2020-01-01 00:00:00'` 
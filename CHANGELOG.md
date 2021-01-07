# BigFlow changelog


## Version 1.1

### Added

* Integration with `pip-tools`
* Configurable timeout for jobs `Job.execution_timeout`
* Flag `Job.depends_on_past` to ignore errors from previous job
* Tools/base classes to simplify writing e2e tests for BigQuery
* `pyproject.toml` to new projects

### Changed

* Project configuration moved to `setup.py`
* The same `setup.py` is used by Beam to build tarballs
* Deprecate some functions at `bigflow.resources` and `bigflow.commons`
* Bigflow uses version ranges for dependencies

### Fixed

* Dataflow jobs starts much faster
* Airflow task waits until Dataflow job is finished
* Fixed `Job.retry_count` and `Job.retry_pause_sec`

## Version 1.0

* Initial release

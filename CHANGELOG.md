# BigFlow changelog

## Version 1.4.1

### Added
 * Optional support for 'pytest' as testing framework
 * Labels support for datasets and tables in DatasetConfig and DatasetManager

## Version 1.4

### Added
 * Check if docker image was pushed before deploying airflow dags

### Fixed
 * Propagate 'env' to bigflow jobs
 * Automatically create and push `:latest` docker tag

## Version 1.3

### Changed

* Changes in building toolchain - optimizes, better logging, optional setup.py

## Version 1.2

### Added

* Tool for synching requirements with Dataflow preinstalled python dependencies.

### Changed

* Schema for 'dirty' (not on git tag or there are local changes) versions was changed.
  Now it includes git commit and git workdir hash instead of random suffix.
* Don't delete intermediate docker layers after build.
* Dockerfile template was changed (does not affect existing projects).

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

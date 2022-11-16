# BigFlow changelog

## Version 1.5.2

### Changed

* Jinja version bumped to >=3<4

### Fixed

* Fixing the issue with producing composer 1.x compatible DAGs (introduced in 1.5.1)

## Version 1.5.1

*Broken!* – DAG builder produces DAGs incompatible with composer 1.x. Fixed in 1.5.2.

### Fixed

* Composer 2.0 support – using `composer-user-workloads` namespace inside generated DAGs if running on Composer 2.X, to fix the problem with inheriting the Composer SA

## Version 1.5.0

### Fixed

* Setting grpcio-status as <=1.48.2 to avoid problems with pip-compile on protobuf package
* changing docker image caching implementation – using BUILDKIT_INLINE_CACHE=1 only if cache properties are set
* always installing typing-extensions>=3.7 to avoid clashes

### Removed

* Deprecated `log` and `dataproc` extras

## Version 1.4.2

### Added

* The `base_frozen` extras with frozen base requirements
* More type hints
* Making exporting image to tar as optional

### Fixed

 * `bf build` arguments validation
 * fixed broken MarkupSafe package version

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

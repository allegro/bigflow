# BigFlow changelog

## Version 1.11.2

### Changed

* Fixed CI/CD release workflow.

### Changed

* Restored `pip-tools` dependency in `requirements/base.txt`,
because it is required for `bf build` command to work properly.
* Limited `wheel` version, as 0.46.1 is broken.
* Upgraded test dockerfiles to python 3.8.

## Version 1.11.1

### Changed

* Made DAG failed import fallbacks more robust.

## Version 1.11.0

### Added

* Support composer-2.10.1-airflow-2.9.3
* Support for development on Apple M3 processors
* Support for Python 3.10

## Version 1.10.0

### Added

* BigQuery query job labeling for collect and write operations. Labels are passed via `job_labels` dict argument in `DatasetConfiguration` and `DatasetManager`.

## Version 1.9.0

### Changed

* Switched from Google Container Registry to Artifact Registry. Made `-r`/`--docker-repository` common for all deploy commands. Build and deploy commands authenticate to the Docker repository taken from `deployment_config.py` or CLI arguments, instead of hardcoded `https://eu.gcr.io`.

## Version 1.8.0

### Changed

* Bumped basic dependencies: Apache Beam 2.48.0, google-cloud-bigtable 2.17.0, google-cloud-language 2.10.0, google-cloud-storage 2.11.2, among others (#374).
* Added the `env_variable` argument to `bigflow.Workflow` which enables to change a name of the variable used to obtain environment name (#365).

## Version 1.7.2

### Fixed

* Fixed compatibility issues with Cloud Composer 2.x and Airflow 2.x

### Breaking changes

* Cloud Composer 2.0.x is not supported properly, please use either 1.x or 2.1+

## Version 1.7.1

### Changed

* Bigflow CLI commands won't fail on additional unknown parameters. This allows to pass additional parameters to BigFlow Jobs. 

## Version 1.7.0

### Changed

* Bumped dependencies of main libraries (e.g. Apache Beam to version 2.45 or BigQuery to version 3.6.0). It enabled compatibility with MacBooks with M1 processor.
* Requires Python version = 3.8

## Version 1.6.0

### Fixed

* Enabled vault endpoint TLS certificate verification by default for `bf build` and `bf deploy` commands. This fixes the MITM attack vulnerability. Kudos to Konstantin Weddige for reporting.

### Breaking changes

* Default vault endpoint TLS certificate verification for `bf build` and `bf deploy` may fail in some environments. Use `-vev`/`--vault-endpoint-verify` option to disable or provide path to custom trusted certificates or CA certificates. Disabling makes execution vulnerable for MITM attacks and is discouraged - do it only when justified and in trusted environments. See [https://requests.readthedocs.io/en/latest/user/advanced/#ssl-cert-verification](https://requests.readthedocs.io/en/latest/user/advanced/#ssl-cert-verification) for details.

## Version 1.5.4

### Changed

* Added two more parameters in KubernetesPodOperator required since Composer 2.1.0

## Version 1.5.3

### Changed

* MarkupSafe bumped to >2.1.0 (avoiding the broken 2.1.0 version)

## Version 1.5.2

### Changed

* Jinja version bumped to >=3<4

### Fixed

* Fixing the DAG builder issue introduced in 1.5.1 – now it produces DAGs compatible with (airflow 1.x + composer 1.x) or (airflow 2.x + composer 2.x)

## Version 1.5.1

*Broken!* – DAG builder produces DAGs incompatible with (airflow 1.x + composer 1.x). Fixed in 1.5.2.

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

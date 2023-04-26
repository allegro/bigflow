# Development

## Branching

We use the [GitFlow](https://datasift.github.io/gitflow/IntroducingGitFlow.html) branching strategy to develop BigFlow.

## Releases

Versioning is driven by the [_version module](../bigflow/_version.py) inside the source code. To change the version
simply modify the variable inside the module and push changes.

Releases are done through [the release workflow](../.github/workflows/release.yml). Release plan can be run manually, 
for example, to release a dev version from branch. Release plan is also run automatically when there is a successful
CI plan run on master (typically after merge).

We use 3 kinds of releases:

* `major.minor.devX` – development releases, for testing purposes
* `major.minor.rcX` – release candidates, tested in a real project
* `major.minor.patch` – standard release, ready to be use in production

## Coding

We cultivate the following rules. Feel free to improve the codebase by the way of development effort.

### Functions visibility

* `_` prefix, e.g. `_do_something()` - the function is private for the module/class that defines it, must not be used anywhere else. Breaking changes allowed.
* no prefix, no decorator, e.g. `do_something()` - cross-module public, i.e. the function is allowed to be used outside the module/class that defines it, but within the BigFlow code. It is not considered a part of the BigFlow API. Breaking changes allowed.
* `@public` decorator, e.g. `@public do_something()` - the function is considered a part of the BigFlow API and can be used by project codebase. **Breaking changes are forbidden.**

### Type hints

Type hints are mandatory for:
* `@public`-decorated functions;
* cross-module public functions.

Type hints are optional for:
* tests;
* private functions.

### Docstrings

* do not double the same content in markdown docs and Python docstrings - skip general descriptions, focus on what's essential when using the class/function;
* docstrings are mandatory for:
  * `@public`-decorated functions;
  * cross-module public functions.

### Pinning base requirements

**When you change a base requirement, remember to update `base_frozen` extras**.
To generate `base_frozen` extras requirements, run the following commands:

```shell script
cd requirements
pip-compile base_frozen.in
```

## Development process

At the beginning set up infrastructure for your test project. You can find description 
for it in [here](https://wiki.allegrogroup.com/display/POGRANICZE/Terraform+in+BigFlow+Projects+Guideline).

Then implement your changes for bigflow and push changes on your newly created specific branch.

After that, on test project set in `requirements.in` bigflow version from your branch. 
to do that you just need to set bigflow in `requirements.in` file like that:

```shell script
# bigflow[bigquery]==1.5.0  <- oryginalna instalacja
git+https://github.com/allegro/bigflow@name-of-your-brancg#egg=bigflow[your-dependencies]
```
and add in your `Dockerfile` following command
```shell script
apt-get install -y git
```

Then remove your `requirements.txt` file,
build dependencies once again (using `pip-compile` or `bf build-requirements` command), then uninstall bigflow locally
and run `pip install -r resources/requirements.txt`. After that, you can simply run `bf build` and `bf deploy` commands 
and finally check created DAGs on Airflow website which you can access from composer who were initialized in infrastructure project.

## Backward compatibility

TODO
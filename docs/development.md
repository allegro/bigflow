# Development

## Branching

We use the [GitFlow](https://datasift.github.io/gitflow/IntroducingGitFlow.html) branching strategy to develop BigFlow.

## Releases

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

## Development process

TODO

## Backward compatibility

TODO
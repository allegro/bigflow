"""This file contains legacy shims to enable building of old (v1.0) bigflow projects"""

import setuptools
from pathlib import Path

from bigflow.commons import public
from bigflow.build.spec import read_project_spec_nosetuppy
from bigflow.build.dist import projectspec_to_setuppy_kwargs, _maybe_dump_setup_params


_reason = "Use `bigflow.build.setup` instead"


@public(deprecate_reason=_reason)
def auto_configuration(project_name: str, project_dir: Path = Path('.').parent, **kwargs) -> dict:
    project_dir = project_dir or Path.cwd()
    return {
        'project_name': project_name,
        'project_dir': project_dir,
        **kwargs,
    }


@public(deprecate_reason=_reason)
def project_setup(project_name, project_dir=None, **kwargs) -> dict:
    _maybe_dump_setup_params({'name': project_name, **kwargs})
    prj = read_project_spec_nosetuppy(project_dir=project_dir, name=project_name, **kwargs)
    return projectspec_to_setuppy_kwargs(prj)


@public(deprecate_reason=_reason)
def default_project_setup(project_name: str, project_dir=None, **kwargs):
    setuptools.setup(**project_setup(**auto_configuration(project_name, project_dir, **kwargs)))

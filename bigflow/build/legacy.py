"""This file contains legacy shims to enable building of old (v1.0) bigflow projects"""

import os
from pathlib import Path

from bigflow.commons import public


_reason = "Use `bigflow.build.setup` instead"


@public(deprecate_reason=_reason)
def auto_configuration(project_name: str, project_dir: Path = Path('.').parent) -> dict:
    return {
        'project_name': project_name,
        'project_dir': project_dir,
    }


@public(deprecate_reason=_reason)
def project_setup(project_name: str) -> dict:
    return {'name': project_name}


@public(deprecate_reason=_reason)
def default_project_setup(project_name: str, project_dir=None):
    if project_dir:
        os.chdir(project_dir)
    import bigflow.build.dist
    return bigflow.build.dist.setup(name=project_name, project_dir=project_dir)

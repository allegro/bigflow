import logging

from pathlib import Path

import bigflow
from bigflow.scaffold.templating import render_builtin_templates


logger = logging.getLogger(__name__)


def start_project(
    is_basic,
    project_name,
    projects_id,
    composers_bucket,
    envs,
    pyspark_job,
):
    project_dir = project_name + '_project'
    project_path = Path(project_dir).resolve()

    render_builtin_templates(
        project_path,
        "new-project",
        variables={
            'is_basic': is_basic,
            'project_id': projects_id[0],
            'dags_bucket': composers_bucket[0],
            'bigflow_version': bigflow.__version__,
            'project_name': project_name,
            'envs': envs,
            'composers_bucket': composers_bucket,
            pyspark_job: True,
        },
    )

    import bigflow.build.pip as bf_pip
    bf_pip.pip_compile(project_path / "resources" / "requirements.txt")


def migrate_project_from_10(
    project_path: Path,
    project_name,
):
    render_builtin_templates(project_path, 'migrate-11', variables={
        'project_name': project_name,
        'bigflow_version': bigflow.__version__,
    })

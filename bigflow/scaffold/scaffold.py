import os
import logging

from pathlib import Path

import bigflow

from bigflow.scaffold.templating import render_builtin_templates
from bigflow.scaffold.scaffold_templates import (
    beam_workflow_template,
    beam_processing_template,
    beam_pipeline_template,
    basic_deployment_config_template,
    advanced_deployment_config_template,
    test_wordcount_workflow_template,
    bq_workflow_template,
)


logger = logging.getLogger(__name__)


def start_project(config):
    templates = format_templates(config)
    create_dirs_and_files(config, templates)


def format_templates(config):
    test_templates = {
        '__init__.py': '',
        'test_wordcount_workflow.py': test_wordcount_workflow_template.format(project_name=config['project_name']),
    }

    deployment_config_template = basic_deployment_config_template.format(
        project_id=config['projects_id'][0],
        dags_bucket=config['composers_bucket'][0])

    if not config['is_basic']:
        for i in range(1, len(config['projects_id'])):
            deployment_config_template = deployment_config_template.strip()
            deployment_config_template += advanced_deployment_config_template.format(env=config['envs'][i], project_id=config['projects_id'][i], dags_bucket=config['composers_bucket'][i])

    main_templates = {
        'deployment_config.py': deployment_config_template,
    }
    beam_templates = {
        'workflow.py': beam_workflow_template,
        'processing.py': beam_processing_template,
        'pipeline.py': beam_pipeline_template % {
            'project_id': config['projects_id'][0],
            'project_name': config['project_name']},
        '__init__.py': '',
    }
    bq_templates = {
        '__init__.py': '',
        'workflow.py': bq_workflow_template % {'project_id': config['projects_id'][0]},
    }
    return {
        'beam_templates': beam_templates,
        'bq_templates': bq_templates,
        'test_templates': test_templates,
        'main_templates': main_templates,
    }


def create_dirs_and_files(config, templates):
    project_dir = config['project_name'] + '_project'
    project_path = Path(project_dir).resolve()
    workflows_path = Path(project_dir) / config['project_name']
    word_count_path = workflows_path / 'wordcount'
    internationalports_path = workflows_path / 'internationalports'
    test_path = Path(project_dir + '/test')

    os.mkdir(project_path)
    os.mkdir(workflows_path)
    os.mkdir(word_count_path)
    os.mkdir(internationalports_path)
    os.mkdir(test_path)

    create_module_files(templates['main_templates'], project_path.resolve())
    create_module_files(templates['beam_templates'], word_count_path.resolve())
    create_module_files(templates['bq_templates'], internationalports_path.resolve())
    create_module_files(templates['test_templates'], test_path.resolve())
    create_module_files({'__init__.py': ''}, workflows_path.resolve())

    render_builtin_templates(
        project_path,
        "new-project",
        variables={
            'project_id': config['projects_id'][0],
            'bigflow_version': bigflow.__version__,
            **config,
        },
    )

    import bigflow.build.pip as bf_pip
    bf_pip.pip_compile(project_path / "resources" / "requirements.txt")


def create_module_files(templates, path):
    for filename, template in templates.items():
        with open(os.path.join(path, filename), 'w+') as f:
            f.write(template)


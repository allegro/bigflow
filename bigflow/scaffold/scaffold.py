import os
from pathlib import Path

from bigflow.scaffold.scaffold_templates import beam_workflow_template, beam_processing_template, \
    beam_pipeline_template, project_setup_template, basic_deployment_config_template, \
    advanced_deployment_config_template, docker_template, basic_beam_config_template, requirements_template, \
    test_wordcount_workflow_template, test_internationalports_workflow_template, \
    bq_workflow_template, bq_processing_template, bq_tables_template, readme_template, advanced_beam_config_template, \
    basic_bq_config_template, advanced_bq_config_template, gitignore_template


def start_project(config):
    templates = format_templates(config)
    create_dirs_and_files(config, templates)


def format_templates(config):
    beam_config_template = basic_beam_config_template.format(project_id=config['projects_id'][0], project_name=config['project_name'])
    bq_config_template = basic_bq_config_template.format(project_id=config['projects_id'][0])
    test_templates = {'__init__.py': '', 'test_wordcount_workflow.py': test_wordcount_workflow_template.format(project_name=config['project_name']), 'test_internationalports_workflow.py': test_internationalports_workflow_template.format(project_name=config['project_name'])}
    resources_templates = {'requirements.txt': requirements_template}
    deployment_config_template = basic_deployment_config_template.format(project_id=config['projects_id'][0], dags_bucket=config['composers_bucket'][0])

    if not config['is_basic']:
        for i in range(1, len(config['projects_id'])):
            deployment_config_template = deployment_config_template.strip()
            deployment_config_template += advanced_deployment_config_template.format(env=config['envs'][i], project_id=config['projects_id'][i], dags_bucket=config['composers_bucket'][i])

            beam_config_template = beam_config_template.strip()
            beam_config_template += advanced_beam_config_template.format(env=config['envs'][i], project_id=config['projects_id'][i], dags_bucket=config['composers_bucket'][i])

            bq_config_template = bq_config_template.strip()
            bq_config_template += advanced_bq_config_template.format(env=config['envs'][i], project_id=config['projects_id'][i])

    beam_config_template = beam_config_template.strip()
    beam_config_template += '.resolve()'

    main_templates = {'project_setup.py': project_setup_template.format(project_name=config['project_name']), 'deployment_config.py': deployment_config_template, 'Dockerfile': docker_template, 'README.md': readme_template.format(project_name=config['project_name'])}
    beam_templates = {'config.py': beam_config_template, 'workflow.py': beam_workflow_template, 'processing.py': beam_processing_template, 'pipeline.py': beam_pipeline_template, '__init__.py': ''}
    bq_templates = {'__init__.py': '', 'config.py': bq_config_template, 'workflow.py': bq_workflow_template, 'processing.py': bq_processing_template, 'tables.py': bq_tables_template}
    return {'beam_templates': beam_templates, 'bq_templates': bq_templates, 'test_templates': test_templates, 'resources_templates': resources_templates, 'main_templates': main_templates}


def create_dirs_and_files(config, templates):
    project_dir = config['project_name'] + '_project'
    project_path = Path(project_dir).resolve()
    workflows_path = Path(project_dir) / config['project_name']
    word_count_path = workflows_path / 'wordcount'
    internationalports_path = workflows_path / 'internationalports'
    resources_path = Path(project_dir + '/resources')
    test_path = Path(project_dir + '/test')

    os.mkdir(project_path)
    os.mkdir(workflows_path)
    os.mkdir(word_count_path)
    os.mkdir(internationalports_path)
    os.mkdir(resources_path)
    os.mkdir(test_path)

    create_module_files(templates['main_templates'], project_path.resolve())
    create_module_files(templates['beam_templates'], word_count_path.resolve())
    create_module_files(templates['bq_templates'], internationalports_path.resolve())
    create_module_files(templates['resources_templates'], resources_path.resolve())
    create_module_files(templates['test_templates'], test_path.resolve())
    create_module_files({'__init__.py': ''}, workflows_path.resolve())
    create_module_files({'.gitignore': gitignore_template}, project_path.resolve())


def create_module_files(templates, path):
    for filename, template in templates.items():
        with open(os.path.join(path, filename), 'w+') as f:
            f.write(template)

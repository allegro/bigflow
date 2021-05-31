import bigflow.build.reflect

spec = bigflow.build.reflect.get_project_spec()
if spec.name != 'bf-selfbuild-project':
    raise AssertionError(spec.name)

# Configuration

Configuration in Bigflow is managed as Python code in `bigflow.Config` objects.
It follows the [Configuration as Code](https://rollout.io/blog/configuration-as-code-everything-need-know/) principle.

You will use `bigflow.Config` to configure individual workflows as well
as to configure GCP deployment of your project.

## Config object

Here is the example of a `Config`:

```python
from bigflow import Config

deployment_config = Config(
   name='dev',
   properties={
       'gcp_project_id': '{env}-project-id',
       'docker_repository_project':  'my-shared-docker-project-id',
       'docker_repository': 'eu.gcr.io/{docker_repository_project}/my-analytics',
       'vault_endpoint': 'https://example.com/vault',
       'dags_bucket': 'europe-west1-my-1234-bucket',
   },
).add_configuration(
    name='prod',
    properties={
        'dags_bucket': 'europe-west1-my-4321-bucket',
    },
)
```

`Config` is the combination of Python `dicts` with
some extra features that are useful for configuring things:

1. The `Config` object holds multiple named configurations, one configuration
per each environment (here `dev` and `prod`).

1. Properties with constant values (here `vault_endpoint`, `docker_repository_project` and `docker_repository`)
are defined only once in the master configuration (here, 'dev' configuration is master).
They are *inherited* by other configurations.

1. Properties with different values per environment (here `dags_bucket`)
are defined explicitly in each configuration.

Test it:

```python
print(deployment_config)
```

final properties:

```text
dev config:
{   'dags_bucket': 'europe-west1-my-1234-bucket',
    'docker_repository': 'eu.gcr.io/my-shared-docker-project-id/my-analytics',
    'docker_repository_project': 'my-shared-docker-project-id',
    'gcp_project_id': 'dev-project-id',
    'vault_endpoint': 'https://example.com/vault'}
prod config:
{   'dags_bucket': 'europe-west1-my-4321-bucket',
    'docker_repository': 'eu.gcr.io/my-shared-docker-project-id/my-analytics',
    'docker_repository_project': 'my-shared-docker-project-id',
    'gcp_project_id': 'prod-project-id',
    'vault_endpoint': 'https://example.com/vault'}
```

You can get config for particular environment by calling `resolve(env)` method.
It return instance of `dict`, which contains all resolved options for specified environment.
Environment may be ommited, in such case `bigflow` will try to resolve current environment name
by inspecting command line arguments and/or os envionment (looking for `bf_env` variable).

```python
config = deployment_config.resolve()
print(config['dags_bucket'])
print(config['gcp_project_id'])
```

### String interpolation

String properties are interpolated using values from other properties.
Thanks to that, your `Config` can be super concise and smart.
For example, the `docker_repository` property is resolved from:

```python
'docker_repository_project':  'my-shared-docker-project-id',
'docker_repository': 'eu.gcr.io/{docker_repository_project}/my-analytics'
```

to

```text
'docker_repository': 'eu.gcr.io/my-shared-docker-project-id/my-analytics'
```


Interpolation is **contextual**, meaning that, it is aware of a current environment. For example:

```python
config = Config(
    name='dev',
    properties={
        'offers': 'fake_offers',
        'transactions': '{offers}_transactions'
    }).add_configuration(
    name='prod',
    properties={
        'offers': 'real_offers'
    })

print(config)
```

final properties:

```text
dev config:
{'offers': 'fake_offers', 'transactions': 'fake_offers_transactions'}
prod config:
{'offers': 'real_offers', 'transactions': 'real_offers_transactions'}

```

There is a **special placeholder**: `env` for properties containing environment name. For example,
the `gcp_project_id` property is resolved from:

```python
'gcp_project_id': '{env}-project-id'
```

to `dev-project-id` on `dev` and to `prod-project-id` on `prod`.

### Master configuration

In a `Config` object you can define a master configuration.
Any property defined in the master configuration is *inherited* by other configurations.

By default, the configuration defined in the `Config` init method is the master one:


```python
config = Config(
    name='dev',
    properties={
        'my_project': 'my_value'
    }).add_configuration(
    name='prod',
    properties={})

print(config)
```

output:

```text
dev config:
{'my_project': 'my_value'}
prod config:
{'my_project': 'my_value'}
```

You can disable properties inheritance by setting the `is_master` flag to `False`:

```python
config = Config(
    name='dev',
    is_master=False,
    properties={
        'my_project': 'my_value'
    }).add_configuration(
    name='prod',
    properties={})

print(config)
```

output:

```text
dev config:
{'my_project': 'my_value'}
prod config:
{}
```

### Default configuration

A default configuration is used when no environment name is given.
By default, the configuration defined in the `Config` init method is the default one.
It is chosen while resolving properties, when `env_name` is `None`. For example:


```python
config = Config(
    name='dev',
    properties={
        'my_project': 'I_am_{env}'
    }).add_configuration(
    name='prod',
    properties={})

print(config.pretty_print())
print(config.pretty_print('dev'))

```

output:

```text
dev config:
{'my_project': 'I_am_dev'}
dev config:
{'my_project': 'I_am_dev'}
```

You can change the roles by setting the `is_default` flag:

```python
config = Config(
    name='dev',
    is_default=False,
    properties={
        'my_project': 'I_am_{env}'
    }).add_configuration(
    name='prod',
    is_default=True,
    properties={})

print(config.pretty_print())
print(config.pretty_print('dev'))
```

output:

```text
prod config:
{'my_project': 'I_am_prod'}
dev config:
{'my_project': 'I_am_dev'}
```

### Operating System environment variables support

**Secrets** shouldn't be stored in source code, but still, you need them in your configuration.
Typically, they are stored safely on CI/CD servers and they are
passed around as Operating System environment variables.

`Config` object supports that approach. When a property is not defined explicitly in Python,
it is resolved from an OS environment variable. For example:


```python
import os
from bigflow import Config

os.environ['bf_my_secret'] = '123456'

config = Config(
    name='dev',
    properties={
        'my_project': 'I_am_{env}'})

print(config)
print('my_secret:', config.resolve()['my_secret'], 'dev')
```

output:

```text
dev config:
{'my_project': 'I_am_dev'}
my_secret: 123456
```

There are two important aspects here.

**First**, OS environment variable names
must be prefixed with `bf_`.

**Second**, since secret properties don't exist in Python code
they are resolved dynamically and automatically added as master configuration.

More interestingly, you can even resolve a current configuration name from OS environment:

```python
import os
from bigflow import Config

config = Config(
    name='dev',
    properties={
        'my_project': 'I_am_{env}'
    }).add_configuration(
    name='prod',
    properties={})

os.environ['bf_env'] = 'prod'
print(config.pretty_print(''))
os.environ['bf_env'] = 'dev'
print(config.pretty_print(''))
```

output:

```text
prod config:
{'my_project': 'I_am_prod'}
dev config:
{'my_project': 'I_am_dev'}
```

## Deployment config

`DeploymentConfig` is a subclass of the `Config` class with one additional, optional argument – `environment_variables_prefix`.
Contrary to the `Config` class, the `DeploymentConfig` allows you to set a custom environment variables name prefix (overriding the default `bf_` value).
It's useful in CI/CD servers. Bamboo is a good example because it adds the `bamboo_` prefix for each environmental variable. 

```python
from bigflow.configuration import DeploymentConfig

deployment_config = DeploymentConfig(
    name='dev',
    environment_variables_prefix='bamboo_bf_',
    properties={{
       'docker_repository': 'your_repository',
       'gcp_project_id': '{project_id}',
       'dags_bucket': '{dags_bucket}'}})
```
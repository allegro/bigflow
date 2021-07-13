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

You can get config for a particular environment by calling `resolve(env)` method.
It returns an instance of `dict`, which contains all resolved options for a specified environment.
The environment may be omitted, in such case, `bigflow` will try to resolve the current environment name
by inspecting command-line arguments and/or os environment (looking for `bf_env` variable).

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


# Class-based configuration [experimental]

Bigflow 1.3 introduces a new class-based approach for keeping configurations.
New API is optional and there is no need to migrate existing code from
`biglfow.Configuration`. However, it allows you to use type hints, which enables
autocompletion in IDEs and gives you more flexibility.

Each configuration is declared as a subclass of `bigflow.konfig.Konfig`.
Properties are declared as attributes, optionally with type hints.
Classes may inherit from each other and share common properties.  Standard
python inheritance rules are used to resolve overridden properties.
Custom methods may be added to classes, however, it is not recommended.

```python
import bigflow.konfig

class CommonConf(bigflow.konfig.Konfig):
    project: str                    # type hint
    region: str = 'europe-west1'    # type hint + default value
    machine_type = 'n2-standard-4'  # only value (type is `typing.Any`)
    num_workers = 4                 # not only strings are allowed

class DevConf(CommonConf):
    project = "dev-project"

class ProdConf(CommonConf):
    project = "prod-project"
    num_workers = 20
```

Config which corresponds to the current environment may be created by function `resolve_konfig`.
It takes dictionary in form of {"environment" -> "config class"} and returns instance of the config.
The `resolve_konfig` function, resolves the configuration based on the `bf_env` environment variable,
same as the "classic" BigFlow configuration tool.

```python
config = bigflow.konfig.resolve_konfig(
    {  # mapping of all configs
        'dev': DevConf,
        'prod': ProdConf,
    },
    default="dev",  # when no environment is provided via CLI
)
print(config.project)      # => "dev-project"
print(config.num_workers)  # => 4
```

Config instance may be used as a dict:

```python
print(config['project'])
print(config.keys())
print(dict(config))
```

Also config may be created manullay in tests or interactive shell:

```python
dev_config = DevConf()
print(dev_config.num_workers)
```

Attributes may be overwritten by passing parameters to the class:

```python
patched_dev_config = DevConf(num_instances=10)
```

### String interpolation

By default string properties are *not* interpolated (it differs from `bigflow.Configuration`).
You need to be explicitly wrap interpolation pattern with function `bigflow.konfig.expand()`:

```python
import bigflow.konfig
from bigflow.konfig import expand

class MyConfig(bigflow.konfig.Konfig):

    docker_repository_project = "my-shared-docker-project-id"

    # no 'expand()' function - string is *not* interpolated
    docker_repository_raw = "eu.gcr.io/{docker_repository_project}/my-analytics"

    # interpolation is enabled
    docker_repository = expand("eu.gcr.io/{docker_repository_project}/my-analytics")


config = MyConfig()
print(config.docker_repository_raw)  # => eu.gcr.io/{docker_repository_project}/my-analytics
print(config.docker_repository)      # => eu.gcr.io/my-shared-docker-project-id/my-analytics
```

### Reading from environment variables

Class-based configs system does not automatically read properties from OS environment.
You must explicitly declare them with `bigflow.konfig.fromenv` function. Note, that the full variable
name must be used, the prefix `bf_` is not prepended automatically.

```python
import bigflow.konfig
from bigflow.konfig import fromenv

import os
os.environ['bf_my_secret'] = '123456'

class MyConfig(bigflow.konfig.Konfig):

    # full variable name "bf_my_secret" is used!
    my_secret = fromenv('bf_my_secret')


config = MyConfig()
print('my_secret:', config.my_secret)
```

### Inheritance

Class-based configs allow defining as many common classes as needed.
There is no single "master config". However, it is _recommended_ to create
a common base class and put type hints for all "public" parameters.

```python
import bigflow.konfig
from bigflow.konfig import expand

class Conf(bigflow.konfig.Konfig):
    """Defines configuration properties and their types"""
    project: str
    region: str
    machine_type: str
    num_machines: int
    dataset: str

class Common(Conf):
    """Shared across all configs"""
    env: str
    project = expand("my-project-{env}")
    region = "europe-west1"
    machine_type = "n2-standard-2"
    num_machines = 10

class Dev(Common):
    """Development environment"""
    env = "dev"
    dataset = "dataset-one"
    num_machines = 2

class DevBench(Dev):
    """Similar to dev, suitable for benchmarks"""
    dataset = "dataset-benchmark"
    num_machines = 15

class Prod(Common):
    """Production environment"""
    env = "prod"
    dataset = "dataset-one"
    num_machines = 15

config = bigflow.konfig.resolve_konfig(
    {
        'prod': Prod,
        'dev': Dev,
        'dev-bench': DevBench,
    },
)

# IDE should be able to autocomplete after `config.`
print(config.num_machines)
```

### Dynamic properties

Configuration properties may be declared as results of some expression:

```python
import bigflow.konfig
from bigflow.konfig import expand, dynamic

class MyConfig(bigflow.konfig.Konfig):
    num_machines = 10
    num_cores = 4
    machine_type = expand("n2-standard-{num_cores}")  # internally `epxand` also uses `dynamic`
    total_cores = dynamic(lambda self: self.num_machines * self.num_cores)

print(dict(MyConfig()))
# => {'num_machines': 10, 'num_cores': 4, 'machine_type': 'n2-standard-4', 'total_cores': 40}

print(dict(MyConfig(num_cores=8)))
# => {'num_machines': 10, 'num_cores': 8, 'machine_type': 'n2-standard-8', 'total_cores': 80}
```

### Mutate configs

Instances of `bigflow.konfig.Konfig` are immutable after instantiation.
A new "changed" instance may be created when only some properties need to be changed:

```python
import bigflow.konfig
from bigflow.konfig import dynamic

class MyConfig(bigflow.konfig.Konfig):
    one = 1
    two = dynamic(lambda self: self.one + 1)

config = MyConfig()
print(config.two)  # => 2

config2 = config.replace(one=101)
print(config.two)  # => 2
print(config2.two) # => 102
```

## Secrets management

To define a secret for your BigFlow workflow, you need to use [Kubernetes Secrets](https://cloud.google.com/composer/docs/how-to/using/using-kubernetes-pod-operator#secret-config) 
mechanism (Cloud Composer is a GKE cluster under the hood). Run the following commands in the [Cloud Shell](https://cloud.google.com/shell/docs/launching-cloud-shell#launching_from_the_console)
or a terminal on your local machine:

```shell script
gcloud composer environments describe YOUR-COMPOSER-NAME --location <YOUR COMPOSER REGION> --format="value(config.gkeCluster)"
gcloud container clusters get-credentials <COMPOSER GKE CLUSTER NAME> --zone <GKE ZONE> --project <GKE PROJECT ID>
kubectl create secret generic <SECRET NAME> --from-literal <SECRET ENVIRONMENT KEY>=<SECRET ENVIRONMENT VALUE>
```

Let us go through a example. First, you need to get the Composer GKE cluster name:

```shell script
gcloud composer environments describe my-composer --location europe-west1 --format="value(config.gkeCluster)"
>>> projects/my-gcp-project/zones/europe-west1-d/clusters/europe-west1-my-composer-6274b78f-gke
```

It's the last element of the whole ID – `europe-west1-my-composer-6274b78f-gke`. Then, you can use the name to fetch the
credentials. You also need to specify the zone and project of your composer:

```shell script
gcloud container clusters get-credentials europe-west1-my-composer-6274b78f-gke --zone europe-west1-d --project my-super-project
```

And you can then set a secret:

```shell script
kubectl create secret generic bf-super-secret --from-literal bf_my_super_secret=passwd1234
```

Next, you need to specify secrets that you want to use in a workflow:

```python
import bigflow

workflow = bigflow.Workflow(
    workflow_id='example', 
    definition=[],
    secrets=['bf_my_super_secret'])
```

And finally, you can use the secret in a job:

```python
import os
import bigflow

class ExampleJob(bigflow.Job):
    id = 'example_job'

    def execute(self, context: bigflow.JobContext):
        os.environ['BF_MY_SUPER_SECRET']

workflow = bigflow.Workflow(
    workflow_id='example', 
    definition=[ExampleJob()],
    secrets=['bf_my_super_secret'])
```

We don't recommend using secrets through the `os.environ`, but instead, through
the `bigflow.konfig.Konfig` configuration class (it makes sure that you can't just log a secret):

```python
class DevConfig(Konfig):
    name = 'dev'
    env_prefix = 'BF_'
    my_super_secret = fromenv('MY_SUPER_SECRET', 'None')
```
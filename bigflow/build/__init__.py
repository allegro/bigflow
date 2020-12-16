def setup(**kwargs):
    """Specify parameters of Bigflow project. Must me called from `setup.py`
    """
    # Laziliy import 'bigflow.dist' as it depends on 'setuptools'
    import bigflow.build.dist as d
    return d.setup(**kwargs)


# TODO: Remove functions in v2.0
import bigflow.build.dist as _dist
from deprecated import deprecated
_reason = "Use `bigflow.build.setup` instead"
default_project_setup = deprecated(reason=_reason)(_dist.default_project_setup)
auto_configuration = deprecated(reason=_reason)(_dist.auto_configuration)
project_setup = deprecated(reason=_reason)(_dist.project_setup)
__all__ = [
    'default_project_setup',
    'auto_configuration',
    'project_setup',
]
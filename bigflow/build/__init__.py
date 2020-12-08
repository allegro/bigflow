__all__ = []

def setup(**kwargs):
    """Specify parameters of Bigflow project. Must me called from `setup.py`
    """
    # Laziliy import 'bigflow.dist' as it depends on 'setuptools'
    import bigflow.build.dist as d
    return d.setup(**kwargs)


# TODO: Remove functions in v2.0
import bigflow.build.dist as _dist
from deprecated import deprecated
for n in [
    'default_project_setup',
    'auto_configuration',
    'project_setup',
]:
    globals()[n] = deprecated(reason="Use `bigflow.build.setup` instead")(getattr(_dist, n))
    __all__.append(n)

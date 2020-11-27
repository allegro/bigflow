# TODO: Remove in v2.0. COMPAT
from bigflow.build.dist import (
    default_project_setup,
    auto_configuration,
    project_setup,
)

def setup(**kwargs):
    """Speficy parameters of Bigflow project. Must me called from `setup.py`
    """
    import bigflow.build.dist as d
    return d.setup(**kwargs)

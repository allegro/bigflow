PROJECT_ID = None
CREDENTIALS = None

try:
    from . import local_config
    PROJECT_ID = local_config.PROJECT_ID
    CREDENTIALS = local_config.CREDENTIALS
except ImportError:
    pass
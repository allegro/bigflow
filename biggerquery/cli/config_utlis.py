from importlib import import_module

from biggerquery.configuration import BiggerQueryConfig


def import_config(config_path):
    object_name = config_path.split('.')[-1]
    module_path = '.'.join(config_path.split('.')[:-1])
    imported_module = import_module(module_path)

    if not hasattr(imported_module, object_name):
        raise BiggerQueryConfigNotFound("Object '%s' not found in module %s" % (object_name, module_path))
    maybe_biggerquery_config = getattr(imported_module, object_name)
    if not isinstance(maybe_biggerquery_config, BiggerQueryConfig):
        raise InvalidConfig("Object '%s' is not BiggerQueryConfig" % config_path)
    return maybe_biggerquery_config


class BiggerQueryConfigNotFound(Exception):
    pass


class InvalidConfig(Exception):
    pass

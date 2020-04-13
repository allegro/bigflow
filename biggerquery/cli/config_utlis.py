from importlib import import_module


def import_config(config_path):
    object_name = config_path.split('.')[-1]
    module_path = '.'.join(config_path.split('.')[:-1])
    imported_module = import_module(module_path)

    if not hasattr(imported_module, object_name):
        raise ValueError("Object '%s' not found in module %s" % (object_name, module_path))
    return getattr(imported_module, object_name)

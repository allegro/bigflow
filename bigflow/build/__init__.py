from bigflow.commons import public

@public()
def setup(**kwargs):
    """Specify parameters of Bigflow project. Must me called from `setup.py`
    """
    # Laziliy import 'bigflow.dist' as it depends on 'setuptools'
    import bigflow.build.dist as d
    return d.setup(**kwargs)


from bigflow.build.legacy import *
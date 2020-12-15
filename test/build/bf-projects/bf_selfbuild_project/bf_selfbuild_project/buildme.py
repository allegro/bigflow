import bigflow.build.reflect as r

def materialize_setuppy():
    return r.materialize_setuppy()

def infer_project_name():
    return r.infer_project_name()

def build_sdist():
    return r.build_sdist()

def build_wheel():
    return r.build_wheel()

def build_egg():
    return r.build_egg()

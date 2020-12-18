# Made 'example_project.main_package' importable as a module
# It is need to complain with bigflow project structure - top-level package X corresponds to project name.
import sys, os.path
sys.path.append(os.path.join(os.path.dirname(__file__), "example_project"))
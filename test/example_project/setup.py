
import setuptools

setuptools.setup(
        name='main_package',
        version='0.1.0',
        packages=setuptools.find_namespace_packages(include=["main_package.*"])
)

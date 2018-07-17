"""
Pypher
------

Pypher is graph abstraction layer that converts Python objects
to Cypher strings complete with bound parameters.
"""
import sys
from setuptools import setup, find_packages


install_requires = [
]

# get the version information
exec(open('neomapper/version.py').read())

setup(
    name = 'python_cypher',
    packages = find_packages(),
    version = __version__,
    description = 'Python Neo4J OGM',
    url = 'https://github.com/emehrkay/neomapper',
    author = 'Mark Henderson',
    author_email = 'emehrkay@gmail.com',
    long_description = __doc__,
    install_requires = install_requires,
    classifiers = [
    ],
    test_suite = 'neomapper.test',
)

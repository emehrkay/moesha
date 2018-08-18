"""
Moesha
------

"""
import sys
from setuptools import setup, find_packages


install_requires = [
]

# get the version information
exec(open('moesha/version.py').read())

setup(
    name = 'moesha',
    packages = find_packages(),
    version = __version__,
    description = '',
    url = 'https://github.com/emehrkay/moesha',
    author = 'Mark Henderson',
    author_email = 'emehrkay@gmail.com',
    long_description = __doc__,
    install_requires = install_requires,
    classifiers = [
    ],
    test_suite = 'moesha.test',
)

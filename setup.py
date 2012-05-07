# -*- coding: utf-8 -*-
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name='yamldb',
    version='0.1',
    url='http://github.com/mitsuhiko/yamldb',
    license='BSD',
    author='Armin Ronacher',
    author_email='armin.ronacher@active-4.com',
    description='Wrapper around YAML and SQLite',
    py_modules=['yamldb'],
    install_requires=['PyYAML>=3.0'],
    include_package_data=True,
    zip_safe=False,
    platforms='any'
)

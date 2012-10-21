#!/usr/bin/env python

import os

try:
      from setuptools import setup
except:
      from distutils.core import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(name='CassandraJsonMapper',
      version='1.0.1',
      description='Simple JSON to Cassandra Python client.',
      long_description=read('README.md'),
      author='Joaquin Casares',
      author_email='joaquin.casares AT gmail.com',
      url='http://www.github.com/joaquincasares/CassandraJsonMapper',
      packages=['CassandraJsonMapper'],
      keywords='apache cassandra json mapper simple client',
      install_requires=['pycassa >1.7.1-2']
     )

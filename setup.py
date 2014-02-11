#!/usr/bin/env python

from distutils.core import setup
from setuptools import find_packages

setup(name='ANAPSID',
      version='3.0',
      description='ANAPSID - An adaptive query processing engine for SPARQL endpoints',
      author='Maribel Acosta, Maria-Esther Vidal, Gabriela Montoya and Simon Castillo',
      author_email='mvidal@ldc.usb.ve',
      url='http://www.github.com/anapsid/anapsid',
      scripts=['scripts/run_anapsid', 'scripts/get_predicates'],
      packages=find_packages(exclude=['docs']),
      include_package_data=True,
      license='GNU/GPL v2'
     )

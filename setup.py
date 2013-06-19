#!/usr/bin/env python

from distutils.core import setup
from setuptools import find_packages

setup(name='ANAPSID',
      version='20130619',
      description='ANAPSID',
      author='',
      author_email='anapsid@ldc.usb.ve',
      url='http://www.github.com/anapsid/anapsid',
      scripts=['scripts/run_anapsid', 'scripts/get_predicates'],
      packages=find_packages(exclude=['docs']),
      include_package_data=True,
     )

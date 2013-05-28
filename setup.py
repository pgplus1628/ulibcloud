import os
import sys

from setuptools import setup
from setuptools import Command
from setuptools.command.install import install as _install
from subprocess import call
from unittest import TextTestRunner, TestLoader




SUPPORTED_VERSIONS = ['2.7']

if sys.version_info <= (2, 4) or sys.version_info >= (3, 0): 
    version = '.'.join([str(x) for x in sys.version_info[:3]]) 
    print('Version ' + version + ' is not supported. Supported versions are ' +
          ', '.join(SUPPORTED_VERSIONS))
    sys.exit(1)



class install(_install):
    
    def initialize_options(self) :
        _install.initialize_options(self)

    def finalize_options(self) :
        _install.finalize_options(self)


setup(
    name = 'ulibcloud',
    version = '0.0.1',
    packages = [
        'ulibcloud',
        'ulibcloud.utils',
        'ulibcloud.ext'
    ],
    package_dir = {'ulibcloud' : 'ulibcloud'},
    install_requires = [
        'apache-libcloud>=0.11.1',
        'zfec>=1.4.24',
    ],
    url = 'https://code.google.com/p/ulibcloud/',
    description = 'A python library providing high availability and global uniform access to cloud storage',
    author = 'zorksylar, msmummy',
    author_email = 'pin.gao2008@gmail.com, msmummy@gmail.com',
    cmdclass = {
        'install' : install,
    },

)

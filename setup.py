#!/usr/bin/env python
import os.path
import sys
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def readme():
    try:
        with open(os.path.join(os.path.dirname(__file__), 'README.md')) as f:
            return f.read()
    except (IOError, OSError):
        return ''


def get_version():
    src_path = os.path.join(os.path.dirname(__file__), 'src')
    sys.path = [src_path] + sys.path
    import pipette
    return pipette.__version__


setup(
    name='pipette',
    version=get_version(),
    description='A library implementing a protocol to simplify programming '
                'pipeline-like chains of intercommunicating processes.',
    long_description=readme(),
    author='Yauhen Yakimovich',
    author_email='eugeny.yakimovitch@gmail.com',
    url='https://github.com/ewiger/pipette/',
    license='MIT',
    packages=[
        'pipette',
    ],
    package_dir = {'':'src'},
    download_url='https://github.com/ewiger/pipette/tarball/master',
    install_requires=[
        'PyYAML>=3.11',
    ],
    classifiers=[
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Development Status :: 5 - Production/Stable',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS',
    ],
)

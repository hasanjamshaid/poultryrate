#!/usr/bin/env python3

import os
from setuptools import setup

# get key package details from py_pkg/__version__.py
about = {}  # type: ignore
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'poultryrate', '__version__.py')) as f:
    exec(f.read(), about)

# load the README file and use it as the long_description for PyPI
with open('README.md', 'r') as f:
    readme = f.read()

# package configuration - for reference see:
# Packages required
REQUIRED = [ 'pandas', 'schedule', 'firebase-admin', 'oauth2client', 'SQLAlchemy', 'PyMySQL' ]


# https://setuptools.readthedocs.io/en/latest/setuptools.html#id9
setup(
    name=about['__title__'],
    description=about['__description__'],
    long_description=readme,
    long_description_content_type='text/markdown',
    version=about['__version__'],
    author=about['__author__'],
    author_email=about['__author_email__'],
    url=about['__url__'],
    packages=['poultryrate'],
    include_package_data=True,
    python_requires=">=3.7.*",
    install_requires=REQUIRED,
    license=about['__license__'],
    scripts=['poultryrate/csv_reader.py',
    'poultryrate/data_model.py',
    'poultryrate/tweet_classifier.py',
    'poultryrate/epakpoultry.py'
    ],
    package_data={'poultryrate': ['poultryrate.cfg']},

    zip_safe=False,
    entry_points={
        'console_scripts': ['poultryrate=poultryrate.poultry_rate_tasks:main'],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    keywords='poultryrate template'
)

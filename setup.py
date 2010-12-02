#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Copyright (c) 2009—2010 Andrey Mikhailenko and contributors
#
#  This file is part of Dark.
#
#  Dark is free software under terms of the GNU Lesser
#  General Public License version 3 (LGPLv3) as published by the Free
#  Software Foundation. See the file README for copying conditions.
#

"Dark setup"

import os
from setuptools import setup, find_packages

from _version import version


def get_readme():
    f = os.path.abspath(os.path.join(os.path.dirname(__file__), 'README'))
    return open(f).read()

setup(
    name         = 'dark',
    version      = version,
    packages     = find_packages(),

    provides     = ['dark'],
    obsoletes    = ['datashaping'],
    requires     = ['python (>= 2.5)', 'doqu (>= 0.25)'],
    test_requires = ['nose', 'pyyaml'],

    description  = 'Data Analysis and Reporting Kit (DARK)',
    long_description = get_readme(),
    author       = 'Andrey Mikhailenko',
    author_email = 'andy@neithere.net',
    url          = 'http://bitbucket.org/neithere/dark/',
    download_url = 'http://bitbucket.org/neithere/dark/src/',
    license      = 'GNU Lesser General Public License (LGPL), Version 3',
    keywords     = 'data analysis mining reporting pivot query database',
    classifiers  = [
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Environment :: Plugins',
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Database :: Front-Ends',
        'Topic :: Office/Business :: Financial :: Spreadsheet',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],

    # release sanity check
    test_suite = 'nose.collector',
)

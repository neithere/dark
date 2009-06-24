#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Copyright (c) 2009 Andy Mikhailenko and contributors
#
#  This file is part of Datashaping.
#
#  Datashaping is free software under terms of the GNU Lesser
#  General Public License version 3 (LGPLv3) as published by the Free
#  Software Foundation. See the file README for copying conditions.
#

"Datashaping setup"

from setuptools import setup, find_packages
import datashaping

long_description = "Datashaping is an engine providing very simple pythonic API"\
                   " allowing for complex queries on structured data."
setup(
    name         = 'datashaping',
    version      = datashaping.__version__,
    packages     = find_packages(exclude=['example_data']),
    
    requires     = ['python (>= 2.5)'],
    provides     = ['datashaping'],
    
    description  = 'A pythonic query API for structured data.',
    long_description = long_description,
    author       = 'Andy Mikhailenko',
    author_email = 'andy@neithere.net',
    url          = 'http://bitbucket.org/neithere/datashaping/',
    download_url = 'http://bitbucket.org/neithere/datashaping/src/',
    license      = 'GNU Lesser General Public License (LGPL), Version 3',
    keywords     = 'query database api',
    classifiers  = [
        'Development Status :: 2 - Pre-Alpha',
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
)

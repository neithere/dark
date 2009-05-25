#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Copyright (c) 2009 Andy Mikhailenko and contributors
#
#  This file is part of Datacasting.
#
#  Datacasting is free software under terms of the GNU Lesser
#  General Public License version 3 (LGPLv3) as published by the Free
#  Software Foundation. See the file README for copying conditions.
#

"Datacasting setup"

from setuptools import setup, find_packages
import datacasting

long_description = "Datacasting is an engine providing very simple pythonic API"\
                   " allowing for complex queries on structured data."
setup(
    name         = 'datacasting',
    version      = datacasting.__version__,
    packages     = find_packages(exclude=['example_data']),
    
    requires     = ['python (>= 2.5)'],
    provides     = ['datacasting'],
    
    description  = 'A pythonic query API for structured data.',
    long_description = long_description,
    author       = 'Andy Mikhailenko',
    author_email = 'andy@neithere.net',
    url          = 'http://bitbucket.org/neithere/datacasting/',
    download_url = 'http://bitbucket.org/neithere/datacasting/src/',
    license      = 'GNU Lesser General Public License (LGPL), Version 3',
    keywords     = 'query database api ',
)

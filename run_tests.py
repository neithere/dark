#!/usr/bin/python -O
# -*- coding: utf-8 -*-

import os
import unittest
import doctest

TESTS_DIRS = ('tests', 'datashaping')

def _test():
    # collect files for testing
    def _add_files(test_files, dirname, fnames):
        for f in fnames:
            if f.endswith('.py') and not f.startswith('__'):
                test_files.append(os.path.join(dirname, f))
    files = []
    for directory in TESTS_DIRS:
        os.path.walk(directory, _add_files, files)

    # set up suite
    suite = unittest.TestSuite()
    for f in files:
        suite.addTest(doctest.DocFileSuite(f))
    runner = unittest.TextTestRunner()
    runner.run(suite)

if __name__ == '__main__':
    _test()

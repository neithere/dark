#!/usr/bin/env python
# -*- coding: utf-8 -*-

from doctest import testmod
from datashaping import db, aggregates, iterating

print 'TESTING DATASHAPING APPLICATION'

testmod(aggregates)
testmod(iterating)
testmod(db)

# prepare for interactive testing.
# use "from test import *" to save on typing
if __name__ != '__main__':
    print
    print '    q = load("filename")  -- returns a Query instance.'
    print '    Console casting function is available as "c".'
    print '    Try  c(q)  to begin with (after you load a file).'

    from datashaping import *

    c = cast_cons
    def load(filename):
        print
        print 'Loading %s data...' % filename
        if filename.endswith('json'):
            from json import load as _load
        else:
            from yaml import load as _load
        data = _load(open('example_data/%s' % filename))
        print 'Data loaded.'
        d = Dataset(data)
        q = d.all()
        return q
    
    #q = load('people')
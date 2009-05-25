#!/usr/bin/env python
# -*- coding: utf-8 -*-

from doctest import testmod
from datacasting import db, aggregates, iterating

print 'TESTING DATACASTING APPLICATION'

testmod(aggregates)
testmod(iterating)
testmod(db)

# prepare for interactive testing.
# use "from test import *" to save on typing
if __name__ != '__main__':
    print 'Reading test data...'
    import yaml
    data = yaml.load(open('example_data/people.yaml'))
    from datacasting import *
    people = Dataset(data)
    q = people.all()
    print '...created dataset "people" and basic query "q". Enjoy!'

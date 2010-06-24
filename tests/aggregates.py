# -*- coding: utf-8 -*-

"""
>>> import yaml
>>> from datashaping.query import Query
>>> from datashaping.storage.memory import MemoryCollection
>>> from datashaping.aggregates import Avg, Count, Max, Median, Min, NA, Qu1, Qu3, Sum
>>> people = Query(storage=MemoryCollection(yaml.load(open('tests/people.yaml'))))

# All aggregates

>>> int(Count().count_for(people))
18
>>> int(Avg('age').count_for(people))
79
>>> int(Min('age').count_for(people))
40
>>> int(Max('age').count_for(people))
232
>>> int(Sum('age').count_for(people))
1272
>>> int(Qu1('age').count_for(people))
99
>>> int(Qu3('age').count_for(people))
45

# N/A policy

>>> str(Sum('age').count_for(people))
'1272'
>>> str(Sum('age', NA.skip).count_for(people))
'1272'
>>> str(Sum('age', NA.reject).count_for(people))
'None'
>>> str(Min('age').count_for(people))
'40'
>>> str(Min('age', NA.skip).count_for(people))
'40'
>>> str(Min('age', NA.reject).count_for(people))
'None'

# Avg

>>> rows = [{'x': 0.5}, {'x': 1.5}]
>>> float(Avg('x').count_for(rows))
1.0

"""

# -*- coding: utf-8 -*-

"""
>>> import datetime
>>> from datashaping.query import Query
>>> from datashaping.storage.memory import MemoryCollection
>>> from datashaping.aggregates import Avg, Count
>>> import yaml
>>> data = yaml.load(open('tests/people.yaml'))
>>> storage = MemoryCollection(data)
>>> storage.inspect() == {'website': 3, 'born': 18, 'name': 18, 'nick': 4,
...                       'age': 16, 'gender': 16, 'occupation': 16,
...                       'fullname': 8, 'residence': 1}
True

#--------------------------------+
# MemoryCollection._unwrap_value |
#--------------------------------+

>>> d = MemoryCollection([])
>>> x = d._unwrap_value('foo', 'bar')
>>> dict(x) == dict([('foo', 'bar')])
True
>>> x = d._unwrap_value('foo', ['bar','quux'])
>>> dict(x) == dict([('foo', 'bar'),
...                  ('foo', 'quux')])
True
>>> x = d._unwrap_value('foo', {'bar': 'quux'})
>>> dict(x) == dict([('foo__bar', 'quux')])
True
>>> x = d._unwrap_value('foo', {'bar': [123, 456]})
>>> dict(x) == dict([('foo__bar', 123),
...                  ('foo__bar', 456)])
True
>>> x = d._unwrap_value('foo', {'bar': {'quux': 123}})
>>> dict(x) == dict([('foo__bar__quux', 123)])
True
>>> x = d._unwrap_value('foo', {'bar': {'quux': [123, 456]}})
>>> dict(x) == dict([('foo__bar__quux', 123),
...                  ('foo__bar__quux', 456)])
True
>>> x = d._unwrap_value('foo', {'bar': [{'quux': [123, 456], 'quack': 789}]})
>>> dict(x) == dict([('foo__bar__quux', 123),
...                  ('foo__bar__quux', 456),
...                  ('foo__bar__quack', 789)])
True
>>> x = d._unwrap_value('foo', datetime.date(2009, 6, 2))
>>> dict(x) == dict([('foo', datetime.date(2009, 6, 2)),
...                  ('foo__year', 2009),
...                  ('foo__month', 6),
...                  ('foo__day', 2)])
True

# >>> x = d._unwrap_value('foo', datetime.datetime(2009, 6, 2, 16, 42))
# >>> dict(x) == dict([('foo', datetime.datetime(2009, 6, 2, 16, 42)),
# ...                  ('foo__year', 2009),
# ...                  ('foo__month', 6),
# ...                  ('foo__day', 2),
# ...                  ('foo__hour', 16),
# ...                  ('foo__minute', 42)])
# True

"""

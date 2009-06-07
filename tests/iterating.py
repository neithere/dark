# -*- coding: utf-8 -*-

"""
>>> from datashaping.iterating import CachedIterator
>>> make_iterator = lambda: CachedIterator(iterable=(x for x in xrange(1,16)), chunk_size=10)
>>> str(make_iterator())
'[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]'
>>> [x for x in make_iterator()]
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]

# CACHED ON __REPR__

>>> eee = make_iterator()
>>> eee._cache
[]
>>> eee
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
>>> eee._cache                   # cache filled after __repr__ call
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
>>> eee._cache = [4,5,6]         # contaminate cache to ensure it is reused
>>> [x for x in eee]
[4, 5, 6]

CACHED ON __ITER__

>>> eee = make_iterator()
>>> [x for x in eee]
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
>>> eee._cache                   # cache filled after __iter__ call
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
>>> eee._cache = 'test'          # contaminate cache to ensure it is reused
>>> eee
test

# CHUNKY BACON! CHUNKY BACON!

>>> eee = make_iterator()
>>> eee[0]                       # indexing is supported
1
>>> eee._cache                   # indexing fills cache up by chunks; requested index is within a chunk
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
>>> eee[1]
2
>>> eee._cache                   # indexing fills cache up by chunks; requested index is within a chunk
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
>>> eee[10]
11
>>> eee._cache                   # index out of chunk borders; next chunk is filled
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]

"""

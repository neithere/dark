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

# Some ideas/code for caching of results are taken from django.db.models.query.QuerySet

__doc__ = """
>>> make_iterator = lambda: CachedIterator(iterable=(x for x in xrange(1,16)))
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

ITER_CHUNK_SIZE = 10 # how many items to cache while iterating

class CachedIterator(object):
    def __init__(self, **kw):
        self._iter  = kw.pop('iterable', None)
        self._cache = []
        self._init(**kw)

    def _init(self, **kw):
        pass

    #------------------------+
    #  Python magic methods  |
    #------------------------+

    __repr__ = lambda self: str(self._to_list())
    __len__  = lambda self: len(self._to_list())

    def __iter__(self):
        if not self._cache:
            self._fill_cache()
        pos = 0
        while 1:
            upper = len(self._cache)
            # iterate over cache
            while pos < upper:
                yield self._cache[pos]
                pos += 1
            # cache exhausted
            if not self._iter:
                # iterable exhausted too
                raise StopIteration
            # refill cache
            self._fill_cache()

    def __getitem__(self, idx):
        # fill cache up to requested index
        upper = len(self._cache)
        if upper <= idx:
            self._fill_cache(idx - upper + ITER_CHUNK_SIZE)
        return self._cache[idx]

    def _prepare_item(self, item):
        "Prepares item just before returning it; can be useful in subclasses."
        return item

    #-------------------+
    #  Private methods  |
    #-------------------+

    def _prepare(self):
        """
        Does not do anything here but can be useful in subclasses to prepare the
        iterable before first iteration, e.g. find intersection between sets, etc.
        """
        pass

    def _to_list(self):
        "Coerces the iterable to list, caches result and returns it."
        self._prepare()
        self._cache = self._cache or [self._prepare_item(x) for x in self._iter]
        return self._cache

    def _fill_cache(self, num=ITER_CHUNK_SIZE):
        """
        Fills the result cache with 'num' more entries (or until the results
        iterator is exhausted).
        """
        self._prepare()
        if self._iter:
            try:
                for i in range(num):
                    self._cache.append(self._prepare_item(self._iter.next()))
            except StopIteration:
                self._iter = None

if __name__=='__main__':
    import doctest
    doctest.testmod()

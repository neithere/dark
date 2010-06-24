# -*- coding: utf-8 -*-
#
#  Copyright (c) 2009 Andy Mikhailenko and contributors
#
#  This file is part of Dark.
#
#  Dark is free software under terms of the GNU Lesser
#  General Public License version 3 (LGPLv3) as published by the Free
#  Software Foundation. See the file README for copying conditions.
#

# Some ideas/code for caching of results are taken from django.db.models.query.QuerySet

ITER_CHUNK_SIZE = 100 # how many items to cache while iterating

class CachedIterator(object):
    def __init__(self, **kw):
        self._iter  = kw.pop('iterable', None)
        self._chunk_size = kw.pop('chunk_size', ITER_CHUNK_SIZE)
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
            self._fill_cache(idx - upper + self._chunk_size)
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

    def _fill_cache(self, num=None):
        """
        Fills the result cache with 'num' more entries (or until the results
        iterator is exhausted).
        """
        self._prepare()
        if self._iter:
            try:
                for i in range(num or self._chunk_size):
                    self._cache.append(self._prepare_item(self._iter.next()))
            except StopIteration:
                self._iter = None

if __name__ == '__main__':
    import doctest
    doctest.testmod()

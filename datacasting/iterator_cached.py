# -*- coding: utf-8 -*-

# Some ideas/code for caching of results are taken from django.db.models.query.QuerySet

ITER_CHUNK_SIZE = 10 # how many items to cache while iterating

class CachedIterator(object):
    def __init__(self, **kw):
        self._iter  = kw.pop('iterable', None)
        self._cache = []
        self.init(**kw)

    def init(self, **kw):
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

def some_iterable():
    for x in 1,2,3:
        yield x

__doc__ = """
>>> str(CachedIterator(some_iterable()))
'[1, 2, 3]'
>>> [x for x in CachedIterator(some_iterable())]
[1, 2, 3]
>>> ci = CachedIterator(some_iterable())
>>> ci._cache
[]
>>> ci
[1, 2, 3]
>>> ci._cache                   # cache filled after __repr__ call
[1, 2, 3]
>>> ci._cache = [4,5,6]   # contaminate cache to ensure it is reused
>>> [x for x in ci]
[4, 5, 6]
>>> ci = CachedIterator(some_iterable())
>>> [x for x in ci]
[1, 2, 3]
>>> ci._cache                   # cache filled after __iter__ call
[1, 2, 3]
>>> ci._cache = 'test'  # contaminate cache to ensure it is reused
>>> ci
test
>>> ci = CachedIterator(some_iterable())
>>> ci[0]                       # indexing is supported
1
>>> ci._cache                   # indexing fills cache up to requested index
[1]
>>> ci[1]
2
>>> ci._cache                   # indexing fills cache up to requested index
[1, 2]
"""

if __name__=='__main__':
    import doctest
    doctest.testmod()

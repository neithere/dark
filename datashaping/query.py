# -*- coding: utf-8 -*-
#
#  Copyright (c) 2009 Andy Mikhailenko and contributors
#
#  This file is part of Datashaping.
#
#  Datashaping is free software under terms of the GNU Lesser
#  General Public License version 3 (LGPLv3) as published by the Free
#  Software Foundation. See the file README for copying conditions.
#

import datetime
from warnings import warn

from utils.iterators import CachedIterator
#from aggregates import *
from storage.base import BaseCollection
from document import Document

__all__ = ['Query']

class Query(CachedIterator):
    """
    A Query instance contains description of a custom query against a storage
    instance. Queries are executed lazily (as late as possible).

    The API closely resembles that of Django.

    :param storage: a :class:`~datashaping.storage.base.BaseCollection` subclass
        instance

    Usage::

        # base query for a storage
        fruits = Query(storage)

        # distinct values for certain key
        kinds = fruits.values_for('kind')

        # filtering data
        apples = fruits.find(kind='apple')

        # chaining queries
        green_apples = apples.find(colour='green')

        # multiple lookups; negation
        yummies = fruits.find(colour='red', taste='sweet').exclude(kind='apple')

        # testing containment
        more_yummies = fruits.find(kind__in=['apple','orange'])

        # comparing numbers
        fresh = more_yummies.find(items_left__gte=1)

        # comparing dates
        fresh = more_yummies.exclude(crop_date__lt=datetime.now())

        # custom grouping
        [(v, q.find(taste=v).values_for('kind')) for v in q.values_for('taste')]

    See tests for more examples.
    """
    # TODO: "or" queries, like q.find(Q(foo=123) | Q(bar=456))
    def _init(self, storage, lookups=None, order_by=None, order_reversed=False, **kw):
        assert isinstance(storage, BaseCollection), 'Query needs a storage to work with'
        self._storage = storage
        self._lookups = lookups or []  # XXX people.find( Q(name='john') | Q(name='mary') ) -- ?
        self._aggregates = []
        self._annotations = {}  # calculated aggregates in form {'items_count': 123}
        #self._group_by = group_by
        self._order_by = {}
        if order_by:
            self._order_by = dict(Query._resolve_ordering_key(k) for k in order_by)
        # cache flags
        self._executed = False
        self._prepared = False
        # cache
        self._ids = None

    #--------------------------+
    #   Python Magic Methods   |
    #--------------------------+


    __repr__ = lambda self: unicode(list(self))

    __len__ = lambda self: len(self._execute())

    #----------------+
    #   Public API   |
    #----------------+

    def count(self):
        """
        Returns the number of documents that match the query lookups.

        This method is a syntactic sugar and is exact synonym for ``__len__``.

        Both ``query.count()`` and ``len(query)`` are equally fast. Both are also faster
        than ``len(list(query))`` because they do not group, sort or wrap data in
        Document objects as ``query.__iter__`` does.
        """
        return len(self)

    def exclude(self, **kw):
        """
        Returns a new :class:`~datashaping.query.Query` instance with
        existing minus given criteria. See :func:`~datashaping.query.Query.find`.
        """
        return self._clone(extra_lookups=[(k, v, True) for k, v in kw.items()])

    def find(self, **kw):
        """
        Returns a new :class:`~datashaping.query.Query` instance with
        existing plus given criteria.

        No query is executed on calling this method. Despite database lookups
        are cheap when the data is stored in the memory, determining intersections
        between subsets can be time-consuming. This is why the query is only
        executed when you really need this, i.e. when you want to know the number
        of results or to iterate over results themselves. The ``find()`` method
        just constructs a new query by copying lightweight metadata.
        """
        return self._clone(extra_lookups=[(k, v, False) for k, v in kw.items()])

    @staticmethod
    def _resolve_ordering_key(key):
        if key[0] == '-':   # a faster way to say .startswith('-')
            key = key[1:]
            reverse = True
        else:
            reverse = False
        return key, reverse

    def order_by(self, *keys):
        return self._clone(order_by=keys)

    def values_for(self, key):
        """
        Returns sorted distinct values for given key filtered by current query.

        This method is a terminal clause, it does not return another lazy
        :class:`~datashaping.query.Query` instance but actually executes
        the query and returns data extracted from the results. However, it's OK
        to call ``query.values_for(...)`` followed by iteration over query itself
        because the results are retrieved only once and then reused.
        """
        if self._lookups:
            ids = self._execute()
            if not ids:
                return []
        else:
            ids = None
        return self._storage.values_for(key, filter_by=ids)

        '''
        # get all existing values for given key
        all_values = self._storage.values_for(key)

        # no filters specified -> do a lightweight dictionary lookup
        if not self._lookups:
            return all_values

        # filters are specified -> execute the query and find intersections
        ids = set(self._execute())
        return sorted(value for value in all_values
                      if ids.intersection(self._storage.ids_by(key,value)))
        '''

    #---------------------+
    #   Private methods   |
    #---------------------+

    def _prepare_item(self, pk):
        """
        Wraps given dictionary in Document object. This allows to represent it as
        integer and retrieve real dictionary lazily on first __getitem__ or __getattr__.
        """
        # XXX if storage is external (e.g. RDBMS), this is highly inefficient!!
        #     should fetch() instead of fetch_one().
        return Document(pk=pk, data=self._storage.fetch_one(pk))#, aggregates=self._calculated_aggregates)

    def _clone(self, extra_lookups=None, extra_aggregates=None,
               group_by=None, order_by=None, order_reversed=False):
        # XXX TODO: if this query was already executed, pass the results to the cloned
        #           and let is just apply new lookups, not do the work from scratch
        lookups = self._lookups + (extra_lookups or [])  # XXX remove duplicates?
        aggregates = self._aggregates + list(extra_aggregates or [])
        #group_by = group_by or self._group_by
        order_by = order_by or self._order_by
        return Query(storage=self._storage, lookups=lookups, aggregates=aggregates, #group_by=group_by,
                     order_by=order_by, order_reversed=order_reversed)

    def _execute(self):
        "Executes the query and returns list of indices of items matching the lookups."
        # TODO: cache results
        #      (it's not that easy as we may return generators; simply saving them
        #      will exhaust the iterator without proper caching, tests will fail.)
        
        if not self._executed:
            # We will iterate over IDs and decorate them with Document objects
            # on the fly using the self._prepare_item() method (called by our
            # superclass).
            if self._order_by:
                self._ids = self._storage.find_ids_sorted(self._lookups,
                                                          self._order_by)
            else:
                self._ids = list(self._storage.find_ids(*self._lookups))
            self._executed = True
        return self._ids

    def _prepare(self):
        """
        Executes the query based on lookups. Annotates, groups, sorts the results
        and returns the iterator.
        """

        if self._prepared:
            return

        self._prepared = True
        ids = self._execute()
        self._iter = iter(ids)

        # annotations
        #if self._aggregates:
        #    i = 0
        #    for agg in self._aggregates:
        #        name = agg.__class__.__name__.lower() +'_'+ agg.key.lower()
        #        while name in self._annotations:
        #            i+=1
        #            name = name + str(i)
        #        self._annotations[name] = agg.aggregate(??????)

        # grouping
        '''
        if self._group_by:
            lookups = {}
            for key in self._group_by:
                lookups[key] = people.values_for('city').keys()
                # lookups['country'] = ['ru', 'uk']
                # lookups['city'] = ['ekb', 'msk', 'lon']
            for key in lookups:
                query = self._storage.all()

                query.find(key
        '''

        # XXX grouping vs. sorting???

    #def aggregate(self, agg):
    #    assert isinstance(agg, Aggregate)
    #    return self.clone(extra_aggregates=[agg])
    '''
    def group_by(self, *args): #, **kw):
        """
        Groups results by given keys. Calculates given aggregates.


        for group in people.all().group_by('country', 'city', 'gender', avg_age=Avg('age')):
            # normal document with aggregate applied to group (i.e. query expressing a "country+city+gender" group)
            print group.country, group.city, group.gender, group.avg_age
        """
        keys = []
        aggs = []
        for arg in args:
            if isinstance(arg, str):
                keys.append(arg)
            elif isinstance(arg, Aggregate):
                aggs.append(arg)
            else:
                raise TypeError, 'expected string or Aggregate instance, got %s' % arg
        assert keys
        return self._clone(group_by=keys, extra_aggregates=aggs)
    '''

if __name__ == '__main__':
    import doctest
    doctest.testmod()

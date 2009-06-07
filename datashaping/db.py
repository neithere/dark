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

from iterating import CachedIterator
from aggregates import *

__all__ = ['Dataset', 'Query']

# LOOKUP_DELIMITER is a delimiter for indexing and subsequent lookups, e.g.:
# "foo__bar" matches {'foo':{'bar':123}}.
# The delimiter can be set to "__" or whatever in order to allow lookups
# by keys containing a dot.
# Note that this will be used as query.find(foo__bar=123), so a little subset
# of symbols is allowed here.
#
# XXX this should be automatically controlled: if current delimiter is found
#     within keys, another must be auto-chosen and data reindexed.
LOOKUP_DELIMITER = '__'
LOOKUP_TYPES = {
    'exact':    lambda value, other:  value == other, # normally direct lookup instead
    'not':      lambda value, other:  value != other,
    'gt':       lambda value, other:  value <  other,
    'lt':       lambda value, other:  other <  value,
    'gte':      lambda value, other:  value <= other,
    'lte':      lambda value, other:  other <= value,
    'in':       lambda value, other:  other in value,  # TODO: date range
    'contains': lambda value, other:  value in other,
    'filled':   lambda value, other:  other is not None,  # defined and not None
    'exists':   lambda value, other:  True,    # any value is OK if it's defined
    # dates:
    'year':     lambda value, other:  value == other.year,
    'month':    lambda value, other:  value == other.month,
    'day':      lambda value, other:  value == other.day,
    'week_day': lambda value, other:  value == other.weekday(),
}

# EXCEPTIONS

class ItemDoesNotMatch(Exception):
    pass

# DOCUMENT

class Document(object):
    "A lazy wrapper for a Dataset item (i.e. a dictionary with numeric identifier)."
    def __init__(self, dataset, idx):   #, annotations):
        assert isinstance(dataset, Dataset), 'Dataset instance expected, got %s' % dataset
        assert isinstance(idx, int), 'integer expected, got %s' % idx
        self._dataset = dataset
        self._idx = idx
        self._dict = None
        #self._annotations = annotations
    get    = lambda self, k,v: self._fetch().get(k,v)
    items  = lambda self: self._fetch().items()
    keys   = lambda self: self._fetch().keys()
    values = lambda self: self._fetch().values()
    __contains__ = lambda self,key: key in self._fetch()
    __eq__       = lambda self,other: isinstance(other, Document) and other._idx == self._idx
    __getitem__  = lambda self,key: self._fetch()[key]
    __hash__     = lambda self: hash(self._idx)
    __int__      = lambda self: self._idx
    __repr__     = lambda self: '<Document %d>' % self._idx
    def __getattr__(self, name):
        if name in self._assign_attrs():
            return self._dict[name]
        raise AttributeError
    def _assign_attrs(self):
        for key, val in self._fetch().items():
            if not key[0] == '_':
                setattr(self, key, val)
        return self._dict
    def _fetch(self):
        if not self._dict:
            self._dict = self._dataset.get_doc(self._idx)
            #for key, val in self._annotations.items():
            #    setattr(self, key, val)
        return self._dict

# DATASET AND QUERY

class Query(CachedIterator):
    """
    A Query instance contains description of a custom query against a Dataset
    instance. Queries are executed lazily: no 
    

    Usage example:
    
    people.find(name='John').exclude(lastname='Connor').values_for('country').annotate(Avg('age'))
    
          people.find(name='john').find(last_name='connor').exclude(location='new york')
                |                 |                       |
     __dataset__|__query__________|__query________________|__query____________________

    TODO: "or" queries, like q.find(Q(foo=123) | Q(bar=456))
    """
    def _init(self, dataset, lookups=None, order_by=None, order_reversed=False, **kw):
        assert isinstance(dataset, Dataset), 'Dataset class provides query methods required for Query to work'
        self._dataset = dataset
        self._lookups = lookups or []  # XXX people.find( Q(name='john') | Q(name='mary') ) -- ?
        self._aggregates = []
        self._annotations = {}  # calculated aggregates in form {'items_count': 123}
        #self._group_by = group_by
        self._order_by = order_by or {}
        self._order_reversed = order_reversed
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

        This method is a syntactic sugar and is exact synonym for __len__.

        Both query.count() and len(query) are equally fast. Both are also faster
        than len(list(query)) because they do not group, sort or wrap data in
        Document objects as query.__iter__ does.
        """
        return len(self)

    def exclude(self, **kw):
        "Returns a new Query instance with existing minus given criteria. See find()."
        return self._clone(extra_lookups=[(k, v, True) for k, v in kw.items()])

    def find(self, **kw):
        """
        Returns a new Query instance with existing plus given criteria.

        No query is executed on calling this method. Despite database lookups
        are cheap when the data is stored in the memory, determining intersections
        between subsets can be time-consuming. This is why the query is only
        executed when you really need this, i.e. when you want to know the number
        of results or to iterate over results themselves. The find() method just
        constructs a new query by copying lightweight metadata.
        """
        return self._clone(extra_lookups=[(k, v, False) for k, v in kw.items()])

    def order_by(self, key):
        if key[0] == '-':   # a faster way to say .startswith('-')
            key = key[1:]
            rev = True
        else:
            rev = False
        return self._clone(order_by=key, order_reversed=rev)

    def values_for(self, key):
        """
        Returns sorted distinct values for given key filtered by current query.

        Method values_for() is a terminal clause, it does not return another lazy
        Query instance but actually executes the query and returns data extracted
        from the results. However, it's OK to call query.values_for(...) followed
        by iteration over query itself because the results are retrieved only once
        and then reused.
        """
        # get all existing values for given key
        all_values = self._dataset.values_for(key)

        # no filters specified -> do a lightweight dictionary lookup
        if not self._lookups:
            return all_values

        # filters are specified -> execute the query and find intersections
        ids = set(self._execute())
        return sorted(value for value in all_values
                      if ids.intersection(self._dataset.ids_by(key,value)))

    #---------------------+
    #   Private methods   |
    #---------------------+

    def _prepare_item(self, item):
        """
        Wraps given dictionary in Document object. This allows to represent it as
        integer and retrieve real dictionary lazily on first __getitem__ or __getattr__.
        """
        return Document(self._dataset, item)#, aggregates=self._calculated_aggregates)

    def _clone(self, extra_lookups=None, extra_aggregates=None,
               group_by=None, order_by=None, order_reversed=False):
        # XXX TODO: if this query was already executed, pass the results to the cloned
        #           and let is just apply new lookups, not do the work from scratch
        lookups = self._lookups + (extra_lookups or [])  # XXX remove duplicates?
        aggregates = self._aggregates + list(extra_aggregates or [])
        #group_by = group_by or self._group_by
        order_by = order_by or self._order_by
        return Query(dataset=self._dataset, lookups=lookups, aggregates=aggregates, #group_by=group_by,
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

            self._ids = list(self._dataset.find_ids(*self._lookups))
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
                query = self._dataset.all()

                query.find(key
        '''

        # XXX grouping vs. sorting???

        # sorting
        if self._order_by:
            #ids = self._to_list()
            self._iter = iter(self._order_results(ids))

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

    def _order_results(self, ids):
        # iterate over all possible distinct values for the key by which we sort
        values = sorted(self._dataset.values_for(self._order_by))
        if self._order_reversed:
            values = reversed(values)
        for value in values:
            value_ids = self._dataset.ids_by(self._order_by, value)
            for i in value_ids:
                if i in ids:
                    yield i
                    ids.pop(ids.index(i))   # distinct

class Dataset(object):
    """
    A query tool for list of dictionaries.
    Currently only 'flat' (i.e. not nested) dictionaries are supported.

    Changing the data
    -----------------

    Dataset is intended for data analysis, not collecting data. This is why
    is does not provide any update/store methods. Dictionaries are not immutable
    but are indexed and cached. If you edit the data within a Dataset instance,
    the index and cache will go out of sync. After and update you will have to
    abandon all existing Query objects because they fetch data once and then
    cache the results until are destroyed themselves. You will also have
    to rebuild Dataset index manually by calling Dataset._build_index().
    This operation is time-consuming, so you will not want to perform it too
    frequently. While it is possible that in the future an updated Document
    instance would automatically efficiently update the corresponding Dataset
    index (remove/add parts), currently it's not the case.
    Also remember to manually save Dataset.data to a file or DB after changing it.
    """
    def __init__(self, data):
        self.data = data
        self._build_index()

    #---------------------+
    #  Proxies for Query  |     XXX remove these to respect loose coupling?
    #---------------------+     dictlist backend can be extracted from the query API.

    def find(self, **kw):
        "Returns a Query instance with dataset items matching given criteria."
        return Query(dataset=self).find(**kw)

    def all(self):
        "Returns a Query instance with all dataset items."
        return self.find()

    #-------------------+
    #  Basic query API  |
    #-------------------+

    def get_doc(self, i):
        "Returns a dictionary by list index."
        try:
            return self.data[i]
        except IndexError:
            raise IndexError, 'tried to access item %d in a dataset which '\
                              'contains only %d items.' % (i, len(self.data))

    def get_docs(self, ids):
        "Generates a list dictionaries by their indices in the main list (data)."
        return (self.data[i] for i in ids)

    def values_for(self, key):
        """
        Returns a sorted list of distinct values existing for given key.
        Caches results for given key. 
        """
        if not key in self._index_values_by_key:
            self._index_values_by_key[key] = sorted( self._index.get(key, {}) )
        return self._index_values_by_key[key]

    def ids_by(self, key, value):
        "Returns indices of items exactly matching given key and value."
        return self._index.get(key, {}).get(value, [])

    def _resolve_lookup_key(self, key):
        "Returns real lookup key and lookup type for given lookup key."
        parts = key.split(LOOKUP_DELIMITER)
        if len(parts) > 1 and parts[-1] in LOOKUP_TYPES:
            lookup_type = parts.pop()
            return LOOKUP_DELIMITER.join(parts), lookup_type
        else:
            return key, 'exact'

    def _get_safe_comparison_value(self, value, other):
        """
        Expects two values. Returns the first one, converted to match the other's
        type (if this is possible). If the value cannot be coerced to a matching
        type, TypeError is raised.
        """
        safe_value = value

        # dates
        if isinstance(other, (datetime.date, datetime.datetime)):
            # if other value is a date, try converting ours to date, too;
            # if this is not possible, the comparison is simply wrong
            # because the data types do not match.
            
            # coerce string to datetime
            if isinstance(value, basestring):
                try:
                    safe_value = datetime.datetime.strptime(value, '%Y-%m-%d')
                except ValueError, e:
                    raise TypeError, 'Could not coerce string to date: %s' % e.message
            # coerce datetime to date
            if isinstance(other, datetime.date):
                try:
                    safe_value = safe_value.date()
                except: pass
        return safe_value

    def find_ids(self, *criteria):
        """
        Returns indices of items matching given criteria.

        A criterion is a triplet: `(lookup, value, negate)`.

        The lookup part
        ---------------

        `lookup` is a string composed of the key by which to search, and the
        desired lookup type. To match nested data structures compound keys can be
        used. Key parts are separated from each other and from the lookup type
        with lookup delimiter which is "__" (double underscore) by default.

        If lookup type is not specified, "exact" is chosen by default.
        Lookup type determines *how* the comparison will be performed.
          
        Examples
        ~~~~~~~~
          
        * `foo`             = key `foo`, lookup type `exact`
        * `foo__exact`      = key `foo`, lookup type `exact`
        * `foo__bar`        = key `foo`, nested key `bar`, lookup type `exact`
        * `foo__bar__exact` = key `foo`, nested key `bar`, lookup type `exact`
        * `foo__in`         = key `foo`, lookup type `in`
        * `foo__bar__in`    = key `foo`, nested key `bar`, lookup type `in`

        If value is empty (e.g. `foo=None`), lookup type `exact` is replaced
        by `filled`. This does not change the meaning but enables more compact
        index table. Please note that lookup types `filled` and `exists` are
        different.

        Lookup types
        ~~~~~~~~~~~~

        * `exact` -- item has exactly the same value for given key. This lookup
          does not involve item-by-item comparison and is thus very fast;
        * `not` -- negated `exact`;
        * `gt`
        * `lt`
        * `gte`
        * `lte`
        * `in` -- item value is a subset of `value`;
        * `contains` -- `value` is a subset of item value;
        * `filled` -- item has given key, value is not empty (not None);
        * `exists` -- item has given key, value can be None.

        Date-related lookup types:

        * `year`
        * `month`
        * `day`
        * `week_day`

        The value part
        --------------

        A `value` makes sense in the context of given lookup type. In most cases
        it serves an example of how certain piece of data should look like.
        This is obviously not the case for boolean lookups such as `filled` and
        `exists`.
        If `value` is a string and in the compared data is of another type,
        `value` is coerced to that type. If this is not possible, TypeError is
        raised.

        The negate part
        ---------------

        `negate` is a boolean. If set to True, the criterion's meaning is inverted.

        A Query constructed like this:
            query.find(first_name='john').exclude(last_name='connor')
        can be represented as criteria triplets this way:
            dataset.find_ids(
                ('first_name', 'john', False),
                ('last_name', 'connor', True),
            )
        """
        if not criteria:
            return iter(xrange(0, len(self.data)))

        # intersecting subsets
        ids_include = None
        ids_exclude = set()
        for key, value, negate in criteria:
            found = []

            # define lookup type and the real lookup key

            key, lookup_type = self._resolve_lookup_key(key)

            # we cannot index all possible keys because document-oriented database
            # can contain too many combinations of keys in comparison to a RDBMS
            # and the index can thus become a storage of empty values.
            # This is why we coerce q.find(foo=None) to q.find(foo__filled=False).
            if lookup_type is 'exact' and value is None:
                lookup_type = 'filled'
                value = False

            # `not` is inverted `exact`
            if lookup_type is 'not':
                lookup_type = 'exact'
                negate = not negate

            # actually find ids

            if lookup_type is 'exact':
                # fast direct lookup
                found.extend(self.ids_by(key, value))
            else:
                if lookup_type in ('exists', 'filled'):
                    # value influences negation, i.e.
                    # "exclude exists=False" means "find exists=True" and so on:
                    negate = negate == value
                # compare our value to each other existing for this key
                for other_value in self.values_for(key):
                    safe_value = self._get_safe_comparison_value(value, other_value)

                    if LOOKUP_TYPES[lookup_type](safe_value, other_value):
                        found.extend(self.ids_by(key, other_value))
            if found:
                if negate:
                    ids_exclude.update(found)
                else:
                    ids_include = ids_include.intersection(found) if ids_include else set(found)
            else:
                # if a condition has no matches, bail out
                return iter([])

        # if nothing was explicitly included, take all items
        if ids_include is None:
            ids_include = set(xrange(0, len(self.data)))

        return iter(ids_include - ids_exclude)

    #-------------------+
    #  Service methods  |
    #-------------------+

    def nest(self, parent_key, key):
        return LOOKUP_DELIMITER.join([parent_key, key])

    def _unwrap_value(self, key, value):
        """
        Unwraps nested data structures. Returns a series of key/value pairs.
        """
        if isinstance(value, (list,tuple)):
            # got multiple values for key,
            # return multiple key/value pairs
            result = []
            for v in value:
                result.extend(self._unwrap_value(key,v))
            return result
        elif isinstance(value, dict):
            # got nested key/value pairs for key,
            # return flattened representation
            results = []
            for k,v in value.items():
                nested_key = self.nest(key,k)
                results.extend(self._unwrap_value(nested_key, v))
            return results
        elif isinstance(value, (datetime.date, datetime.datetime)):
            # got date,
            # return it as is and also decompose up to day or minute
            result = [
                (key, value),
                (self.nest(key,'year'),  value.year),
                (self.nest(key,'month'), value.month),
                (self.nest(key,'day'),   value.day),
                #(self.nest(key,'week_day'), value.weekday()),
            ]
            # XXX this is redundant in most cases; can be indexed on request or
            #     if some options/metadata is provided; think about it later.
            #if isinstance(value, datetime.datetime):
            #    result.extend([
            #        (self.nest(key,'hour'),   value.hour),
            #        (self.nest(key,'minute'), value.minute),
            #    ])
            return result
        else:
            # got single value,
            # return key/value pair (wrapped in list)
            if not isinstance(value, (basestring, int, float, bool, type(None))):
                warn(u'Expected string, number, boolean or None, got %s while '\
                     'indexing data.' % value, UserWarning)
            return [(key, value)]

    def _build_index(self):
        "Creates index for data. Representation: key -> value -> list_of_IDs."
        if __debug__: print 'building index...'

        self._index = {}

        for i, item in enumerate(self.data):
            for key in item:
                val = item[key]
                for k, v in self._unwrap_value(key, val):
                    self._index.setdefault(k, {}).setdefault(v, []).append(i)

        # reset other indices built ad hoc
        self._index_values_by_key = {}

        if __debug__: print 'done.'

    def inspect(self):
        "Shown how many times each key is found in the dataset."
        keys = {}
        for item in self.data:
            for key in item.keys():
                if item[key] != None:
                    keys[key] = keys.setdefault(key, 0) + 1
        return keys

    #------------------+
    #  XXX  TODO  XXX  |
    #------------------+

    '''
    def group_by(self, *keys):
        "Returns items with existing given keys, grouped by these keys."
        #grouped = {}
        #for key in keys:

        grouped = {}  # {'grouper1': {'grouper2': [1,5,7] } }
        lookups = dict((k,any_) for k in keys)
        print lookups
        matches = self.find(**lookups)
        for i in matches:
            print '  i', i
            for key in keys:
                print '    key', key
                # nested groups
                value = self.data[i][key]
                print '      value', value
                grouped.setdefault(key, {}).setdefault(value, []).append(i)
                print '      grouped', grouped
        #return grouped
        return {'all': matches, 'grouped': grouped}
    '''

if __name__ == '__main__':
    import doctest
    doctest.testmod()

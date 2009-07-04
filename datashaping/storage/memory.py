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
from datashaping.storage.base import BaseCollection, BaseCondition

__all__ = ['MemoryCollection', 'Condition']

class MemoryCollection(BaseCollection):
    """
    A query tool for a list of dictionaries wholly loaded into memory.
    Can be loaded from JSON, YAML, CSV, etc.

    Nested lists and dictionaries are supported.
    """
    def __init__(self, data):
        self.data = data if isinstance(data, (list,tuple)) else list(data)
        self._build_index()

    #-------------------+
    #  Basic query API  |
    #-------------------+

    def delete(self, ids, refresh_index=True):
        """
        Deletes items with given primary keys from the storage.

        :param ids: list of primary keys to be removed from the storage
        :param refresh_index: if `True` (default), storage index will be rebuilt
            after removing of items. Set it to `False` to save on this process,
            but make sure not to query by fields that existed in the deleted
            items because queries on outdated index may return broken data.

        """
        # check if all given primary keys exist
        try:
            [self.data[pk] for pk in ids]
        except IndexError:
            raise IndexError('Could not delete documents from storage: wrong '
                             'primary key(s) provided (%s)' % ids)
        # delete them (i.e. replace data with empty dictionaries)
        for pk in ids:
            self.data[pk] = { }
        if refresh_index:
            # TODO: do not rebuild the whole index, just remove certain values
            #       from index if they contain only references to removed items
            self._build_index()

    def fetch(self, ids):
        "Returns a list of dictionaries filtered by their indices in the collection."
        return (self.data[i] for i in ids)

    def fetch_one(self, i):
        "Returns a dictionary by list index."
        try:
            return self.data[i]
        except IndexError:
            raise IndexError, 'tried to access item %d in a collection which '\
                              'contains only %d items.' % (i, len(self.data))

    def find_ids(self, *conditions):
        """
        Returns indices of items matching given criteria.

        :param conditions: an optional list of
            :class:`~datashaping.storage.dataset.Condition` instances.

        A Query constructed like this::

            query.find(first_name='john').exclude(last_name='connor')

        can be represented as conditions this way::

            collection.find_ids(
                Condition('first_name', 'john', False),
                Condition('last_name', 'connor', True),
            )
        """
        if not conditions:
            return iter(xrange(0, len(self.data)))

        # intersecting subsets
        ids_include = None
        ids_exclude = set()
        for c in conditions:
            found = []

            # convert list or tuple to Condition (XXX move class to app-wide module?)
            if not isinstance(c, Condition):
                c = Condition(*c)

            # define lookup type and the real lookup key

            key, lookup_type, value, negate = c.key, c.lookup_type, c.value, c.negate

            # we cannot index all possible keys because document-oriented database
            # can contain too many combinations of keys in comparison to a RDBMS
            # and the index can thus become a storage of empty values.
            # This is why we coerce q.find(foo=None) to q.find(foo__filled=False).
            if lookup_type is 'exact' and c.value is None:
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

                    if Condition.LOOKUP_TYPES[lookup_type](safe_value, other_value):
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

    def find_ids_sorted(self, conditions, order_by):
        """
        Wrapper for `~datashaping.storage.dataset.find_ids`, allows to sort items
        by given keys and, optionally, to reverse the order.

        :param order_by: a dictionary in the form ``{key: reverse}`` where
            ``key`` is a data keys by which to sort, and ``reverse`` is boolean
            which, being True, reverses the ordering by this key.
        """
        # TODO: sorting by nested keys, e.g. born__country
        ids = list(self.find_ids(*conditions))
        for key, reverse in order_by.items():
            ids.sort(key=lambda idx: self.fetch_one(idx).get(key, None),
                     reverse=reverse)
        return ids

    def ids_by(self, key, value):
        "Returns indices of items exactly matching given key and value."
        return self._index.get(key, {}).get(value, [])

    def inspect(self):
        "Shows how many times each key is found in the collection."
        keys = {}
        for item in self.data:
            for key in item.keys():
                if item[key] != None:
                    keys[key] = keys.setdefault(key, 0) + 1
        return keys

    def values_for(self, key, filter_by=None):
        """
        Returns a sorted list of distinct values existing for given key.
        Caches results for given key.
        If `filter_by` (a list of integers) is specified, returns only values that
        exist for these ids.
        """
        # get and cache lookup
        if not key in self._index_values_by_key:
            self._index_values_by_key[key] = sorted( self._index.get(key, {}) )
        ids = self._index_values_by_key[key]

        if not filter_by:
            return ids

        filter_by = filter_by if isinstance(filter_by, set) else set(filter_by)
        return [value for value in ids
                if filter_by.intersection(self.ids_by(key,value))]

    #-------------------+
    #  Service methods  |
    #-------------------+

    def _build_index(self):
        "Creates index for data. Representation: key -> value -> list_of_IDs."
        #if __debug__: print 'building index...'

        self._index = {}

        for i, item in enumerate(self.data):
            for key in item:
                val = item[key]
                for k, v in self._unwrap_value(key, val):
                    self._index.setdefault(k, {}).setdefault(v, []).append(i)

        # reset other indices built ad hoc
        self._index_values_by_key = {}

        #if __debug__: print 'done.'

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

    def _nest(self, parent_key, key):
        return Condition.LOOKUP_DELIMITER.join([parent_key, key])

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
                nested_key = self._nest(key,k)
                results.extend(self._unwrap_value(nested_key, v))
            return results
        elif isinstance(value, (datetime.date, datetime.datetime)):
            # got date,
            # return it as is and also decompose up to day or minute
            result = [
                (key, value),
                (self._nest(key,'year'),  value.year),
                (self._nest(key,'month'), value.month),
                (self._nest(key,'day'),   value.day),
                #(self._nest(key,'week_day'), value.weekday()),
            ]
            # XXX this is redundant in most cases; can be indexed on request or
            #     if some options/metadata is provided; think about it later.
            #if isinstance(value, datetime.datetime):
            #    result.extend([
            #        (self._nest(key,'hour'),   value.hour),
            #        (self._nest(key,'minute'), value.minute),
            #    ])
            return result
        else:
            # got single value,
            # return key/value pair (wrapped in list)
            if not isinstance(value, (basestring, int, float, bool, type(None))):
                warn(u'Expected string, number, boolean or None, got %s while '\
                     'indexing data.' % value, UserWarning)
            return [(key, value)]

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

class Condition(BaseCondition):
    pass

if __name__ == '__main__':
    import doctest
    doctest.testmod()

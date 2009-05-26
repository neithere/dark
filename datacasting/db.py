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

from iterating import CachedIterator
from aggregates import *

__doc__ = """
>>> import datetime
>>> from aggregates import Avg, Count
>>> import yaml
>>> data = yaml.load(open('example_data/people.yaml'))
>>> people = Dataset(data)
>>> people.inspect() == {'website': 2, 'city': 14, 'name': 14, 'nick': 4, 'country': 14, 'age': 13, 'born': 13, 'gender': 14, 'occupation': 12}
True
>>> len(people.all())   # results are found
14
>>> len(people.all())   # iterator is not exhausted
14
>>> people.find()[3]
<Document 3>
>>> len(people.find(country=any_))
14
>>> len(people.find(occupation=any_))
12
>>> len(people.find(age=not_(None)))
13
>>> len(people.all().exclude(age=None))     # same as previous, different syntax
13
>>> people.all().exclude(age=None).count()  # same as previous, yet another syntax
13
>>> people.find(country='England')
[<Document 8>, <Document 11>]
>>> repr(people.find(country='England'))
'[<Document 8>, <Document 11>]'
>>> [p.name for p in people.find(country='England')]
['Thomas Fowler', 'Alan Mathison Turing']
>>> len(people.find(country=exact('England')))      # same as above but faster
2
>>> len(people.find(country=not_('USA')))
8
>>> [p.name for p in people.find(country='USA', gender=not_('male'))]    # multiple conditions
['Kathleen Antonelli', 'Jean Bartik']
>>> [p.name for p in people.find(country='USA').exclude(gender='male')]  # same but cleaner
['Kathleen Antonelli', 'Jean Bartik']
>>> item = people.all()[0]
>>> item
<Document 0>
>>> item._dict == None   # empty dict
True
>>> item.name
'Richard M. Stallman'
>>> item._dict == {'name': 'Richard M. Stallman',
...                'nick': 'rms',
...                'born': datetime.date(1953, 3, 16),
...                'age': 56,
...                'gender': 'male',
...                'country': 'USA',
...                'city': 'New York',
...                'occupation': 'President of the FSF',
...                'website': 'http://stallman.org'}
True
>>> people.values_for('country')
['England', 'Finland', 'Netherlands', 'New Zealand', 'Norway', 'Sweden', 'Switzerland', 'USA']

# group by country
>>> for country in people.values_for('country'):
...     print country
England
Finland
Netherlands
New Zealand
Norway
Sweden
Switzerland
USA

# group by country; calculate average age
>>> for country in people.values_for('country'):
...     documents = people.find(country=country)
...     print country, str(Avg('age').count_for(documents))
England 164.5
Finland 40.0
Netherlands 79.0
New Zealand N/A
Norway 83.0
Sweden 102.0
Switzerland 75.0
USA 70.0

# group by country; count country population
>>> for country in sorted(people.values_for('country')):
...     documents = people.find(country=country)
...     print country, int(Count().count_for(documents))
England 2
Finland 1
Netherlands 1
New Zealand 1
Norway 1
Sweden 1
Switzerland 1
USA 6

# group by country and city
>>> for country in people.values_for('country'):
...     for city in people.find(country=country).values_for('city'):
...         print country, '-', city
England - Great Torrington
England - Maida Vale, London
Finland - Helsinki
Netherlands - Rotterdam
New Zealand - Auckland
Norway - Oslo
Sweden - Stockholm
Switzerland - Winterthur
USA - Betty Jean Jennings
USA - Chicago
USA - Milwaukee
USA - New York
USA - None
USA - San Jose

# group by country and city; count city population
>>> for country in people.values_for('country'):
...     for city in people.find(country=country).values_for('city'):
...         documents = people.find(country=country, city=city)
...         print '%s, %s (%d)' % (country, city, Count().count_for(documents))
England, Great Torrington (1)
England, Maida Vale, London (1)
Finland, Helsinki (1)
Netherlands, Rotterdam (1)
New Zealand, Auckland (1)
Norway, Oslo (1)
Sweden, Stockholm (1)
Switzerland, Winterthur (1)
USA, Betty Jean Jennings (1)
USA, Chicago (1)
USA, Milwaukee (1)
USA, New York (1)
USA, None (1)
USA, San Jose (1)

# group by country, city and gender
>>> for country in people.values_for('country'):
...     for city in people.find(country=country).values_for('city'):
...         for gender in people.find(country=country, city=city).values_for('gender'):
...             print '%s, %s, %s' % (country, city, gender)
England, Great Torrington, male
England, Maida Vale, London, male
Finland, Helsinki, male
Netherlands, Rotterdam, male
New Zealand, Auckland, male
Norway, Oslo, male
Sweden, Stockholm, male
Switzerland, Winterthur, male
USA, Betty Jean Jennings, female
USA, Chicago, male
USA, Milwaukee, male
USA, New York, male
USA, None, female
USA, San Jose, male

# group by country, city, and gender; calculate average age
>>> for country in people.values_for('country'):
...     for city in people.find(country=country).values_for('city'):
...         for gender in people.find(country=country, city=city).values_for('gender'):
...             documents = people.find(country=country, city=city, gender=gender)
...             print 'Average %s from %s, %s is %s years old' % (gender, city, country, str(Avg('age').count_for(documents)))
Average male from Great Torrington, England is 232.0 years old
Average male from Maida Vale, London, England is 97.0 years old
Average male from Helsinki, Finland is 40.0 years old
Average male from Rotterdam, Netherlands is 79.0 years old
Average male from Auckland, New Zealand is N/A years old
Average male from Oslo, Norway is 83.0 years old
Average male from Stockholm, Sweden is 102.0 years old
Average male from Winterthur, Switzerland is 75.0 years old
Average female from Betty Jean Jennings, USA is 85.0 years old
Average male from Chicago, USA is 60.0 years old
Average male from Milwaukee, USA is 72.0 years old
Average male from New York, USA is 56.0 years old
Average female from None, USA is 88.0 years old
Average male from San Jose, USA is 59.0 years old
"""

# EXCEPTIONS

class ItemDoesNotMatch(Exception):
    pass

# SPECIAL VALUES FOR LOOKUP

class SpecialValue(object):
    pass

class SpecialContainerValue(SpecialValue):
    def __init__(self, value):
        self._value = value
    __repr__ = lambda self: u'<%s %s>' % (self.__class__.__name__, self._value)

class not_(SpecialContainerValue):
    "Any value but this. Usage: not_(1) == 1 (False), not_(1) == 2 (True), not_(1) != 1 (True), not_(1) != 2 (False)"
    __eq__ = lambda self,other: self._value != other
    __ne__ = lambda self,other: not self.__eq__(other)

any_ = not_(None)  # XXX actually "any" means *any*, including nulls :) this one must be renamed to "NotNull" or whatever

class in_(SpecialContainerValue):
    __eq__ = lambda self,other: self._value.__contains__(other)

class exact(SpecialContainerValue):
    "Does not add any special behaviour to contained value but allows faster lookups."
    __eq__ = lambda self,other: self._value == other

class gt(SpecialContainerValue):
    "EXPERIMENTAL"\
    " people.find(born=gt('1900-01-01')) --> all people born since 1st January 1900"
    def __eq__(self, other):
        # special case: date
        if isinstance(other, (datetime.date, datetime.datetime)):
            if not ininstance(self._value, (datetime.date, datetime.datetime)):
                v = datetime.date(self._value)
                return other < v
        # all other cases
        else:
            return other < self._value

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
    values = lambda self: self._fetch().values_for()
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

    TODO: multi-value lookups ("name=['john','mary']" -- compare list or items in list? maybe "name=in_(['john','mary')"?)
    TODO: date lookups (year, month, day, time, range, cmp)
    """
    def _init(self, dataset, lookups={}, order_by={}, order_reversed=False, **kw):
        assert isinstance(dataset, Dataset), 'Dataset class provides query methods required for Query to work'
        self._dataset = dataset
        self._lookups = lookups  # XXX people.find(name='john').find(name='mary') -- ?
        self._aggregates = []
        self._annotations = {}  # calculated aggregates in form {'items_count': 123}
        #self._group_by = group_by
        self._order_by = order_by
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
        "Returns a new Query instance with existing minus given lookups. See find()."
        inverted_lookups = dict((k,not_(v)) for k,v in kw.items())
        return self.find(**inverted_lookups)

    def find(self, **kw):
        """
        Returns a new Query instance with existing plus given lookups.

        No query is executed on calling this method. Despite database lookups
        are cheap when the data is stored in the memory, determining intersections
        between subsets can be time-consuming. This is why the query is only
        executed when you really need this, i.e. when you want to know the number
        of results or to iterate over results themselves. The find() method just
        constructs a new query by copying lightweight metadata.
        """
        return self._clone(extra_lookups=kw)

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
        if not self._lookups:
            return self._dataset.values_for(key)
        # get docs for current lookups
        docs = self._dataset.get_docs(self._execute())
        # gather values
        values = []
        for d in docs:
            if key in d:
                values.extend(self._dataset._unwrap_value(d[key]))
        # make them distinct
        return sorted(set(values)) # returns list, not set

    #---------------------+
    #   Private methods   |
    #---------------------+

    def _prepare_item(self, item):
        """
        Wraps given dictionary in Document object. This allows to represent it as
        integer and retrieve real dictionary lazily on first __getitem__ or __getattr__.
        """
        return Document(self._dataset, item)#, aggregates=self._calculated_aggregates)

    def _clone(self, extra_lookups={}, extra_aggregates=[], group_by=[], order_by=None, order_reversed=False):
        # XXX TODO: if this query was already executed, pass the results to the cloned
        #           and let is just apply new lookups, not do the work from scratch
        lookups = self._lookups.copy()
        lookups.update(**extra_lookups)
        aggregates = self._aggregates + list(extra_aggregates)
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

            self._ids = list(self._dataset.find_ids(**self._lookups))
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
        if __debug__: print 'initializing dataset...'
        self.data = data
        self._build_index()
        #self._rebuild_vectors()

    #---------------------+
    #  Proxies for Query  |
    #---------------------+

    def find(self, **kw):
        "Returns a Query instance with dataset items matching given criteria."
        return Query(dataset=self, lookups=kw)

    def all(self):
        "Returns a Query instance with all dataset items."
        return self.find()

    #-------------------+
    #  Basic query API  |
    #-------------------+

    def get_doc(self, i):
        "Returns a dictionary by list index."
        return self.data[i]

    def get_docs(self, ids):
        "Generates a list dictionaries by their indices in the main list (data)."
        return (self.data[i] for i in ids)

    def values_for(self, key):
        "Returns a sorted list of distinct values existing for given key."
        return self._index_values_by_key.setdefault(key,
                       sorted( self._index.get(key, {}) ))

    def ids_by(self, key, value):
        "Returns indices of items exactly matching given key and value."
        return self._index.get(key, {}).get(value, [])

    def find_ids(self, **kw):
        "Returns indices of items matching given criteria."
        ids = None
        if not kw:
            return iter(xrange(0, len(self.data)))
        # intersecting subsets
        for key, value in kw.items():
            found = []
            if isinstance(value, exact):
                # exact match allowed, cut off a corner, grab dict keys and leave
                found.extend(self.ids_by(key, value._value))
            else:
                # gotta compare our value to each other
                for other_value in self.values_for(key):
                    # TODO: (==, <, >, >=, <=) for scalars and dates
                    if value == other_value:
                        # NOTE: get by the other value because ours can be a pseudo-value
                        found.extend(self.ids_by(key, other_value))
            if found:
                ids = ids.intersection(found) if ids else set(found)
        return [] if ids is None else iter(ids)

    #-------------------+
    #  Service methods  |
    #-------------------+

    def _unwrap_value(self, value):
        "Wraps scalar in list; if value already was a list, it's returned intact."
        # (one level only. if there are more, will raise TypeError: unhashable type)
        if isinstance(value, list):
            return value
        elif isinstance(value, dict):
            raise TypeError, 'This program cannot correctly process nested dictionaries within documents, like this one: %s' % unicode(dict)
        else:
            return [value]
        #except TypeError:
        #    raise TypeError, 'could not index %s=%s' % (key, val)

    def _build_index(self):
        "Creates index for data. Representation: key -> value -> list_of_IDs."
        if __debug__: print 'building index...'

        self._index = {}

        for i, item in enumerate(self.data):
            for key in item:
                val = item[key]
                if val != None:
                    for v in self._unwrap_value(val):
                        self._index.setdefault(key, {}).setdefault(v, []).append(i)

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

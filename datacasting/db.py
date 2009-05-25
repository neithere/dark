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
>>> people.values('country')
['England', 'Finland', 'Netherlands', 'New Zealand', 'Norway', 'Sweden', 'Switzerland', 'USA']

# group by country
>>> for country in people.values('country'):
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
>>> for country in people.values('country'):
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
>>> for country in sorted(people.values('country')):
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
>>> for country in people.values('country'):
...     for city in people.find(country=country).values('city'):
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
>>> for country in people.values('country'):
...     for city in people.find(country=country).values('city'):
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
>>> for country in people.values('country'):
...     for city in people.find(country=country).values('city'):
...         for gender in people.find(country=country, city=city).values('gender'):
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
>>> for country in people.values('country'):
...     for city in people.find(country=country).values('city'):
...         for gender in people.find(country=country, city=city).values('gender'):
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

# DOCUMENT

class Document(object):
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
            self._dict = self._dataset.doc_by_id(self._idx)
            #for key, val in self._annotations.items():
            #    setattr(self, key, val)
        return self._dict

# DATASET AND QUERY

class Query(CachedIterator):
    """
    people.find(name='John').exclude(lastname='Connor').values('country').annotate(Avg('age'))
    
          people.find(name='john').find(last_name='connor').exclude(location='new york')
                |                 |                       |
     __dataset__|__query__________|__query________________|__query____________________

     Currently there's only find() method because any exclude(foo=123) is just find(foo=not_(123)).

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
        self._prepared = False

    # XXX this is *wrong*: it displays query.full() but __getitem__ and __iter__ return only IDs!
    __repr__ = lambda self: str(' '.join(str(x) for x in self))


    def _prepare_item(self, item):
        "wrap dicts in objects allowing to represent them as indices and retrieve real dicts lazily on first __getitem__ or __getattr__"
        return Document(self._dataset, item)#, aggregates=self._calculated_aggregates)

    def _clone(self, extra_lookups={}, extra_aggregates=[], group_by=[], order_by=None, order_reversed=False):
        lookups = self._lookups.copy()
        lookups.update(**extra_lookups)
        aggregates = self._aggregates + list(extra_aggregates)
        #group_by = group_by or self._group_by
        order_by = order_by or self._order_by
        return Query(dataset=self._dataset, lookups=lookups, aggregates=aggregates, #group_by=group_by,
                     order_by=order_by, order_reversed=order_reversed)

    def find(self, **kw):
        "Returns a new Query instance with existing plus given lookups."
        return self._clone(extra_lookups=kw)

    def exclude(self, **kw):
        "Returns a new Query instance with existing minus given lookups."
        inverted_lookups = dict((k,not_(v)) for k,v in kw.items())
        return self.find(**inverted_lookups)

    def order_by(self, key):
        if key[0] == '-':   # a faster way to say .startswith('-')
            key = key[1:]
            rev = True
        else:
            rev = False
        return self._clone(order_by=key, order_reversed=rev)

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

    def values(self, key, flat=False):
        "Returns sorted distinct values for given key filtered by current query."
        val_to_id = {}
        for i in self._dataset.ids_by_lookups(**self._lookups):
            orig_value = self._dataset.doc_by_id(i).get(key)
            for value in self._dataset._unwrap_value(orig_value):
                val_to_id.setdefault(value,[]).append(i)
        return val_to_id and (flat and sorted(val_to_id).keys() or sorted(val_to_id)) or []

    def count(self):
        """Returns number of documents that match the query.
        Note that query.count() is a convenience method, it is not__ faster than len(query)
        because we must iterate all existing items in order to make non-exact comparison.
        """
        return len(self)

    def _prepare(self):
        "Executes the query based on lookups"
        if not self._prepared:
            if self._lookups:
                # collect sets of document IDs using this query's lookups
                sets = []
                for k,v in self._lookups.items():
                    ids = self._dataset.ids_by_lookups(**{k:v})
                    sets.append(ids)
                # save intersection between all gathered sets as iterator
                # (we cannot just merge the lists of IDs because it's not OR but AND)
                if len(sets) == 1:
                    self._iter = sets[0]
                else:
                    self._iter = reduce(lambda s1,s2: iter(set(s1) & set(s2)), sets)
            else:
                self._iter = self._dataset.ids_by_lookups()
            self._prepared = True

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
                    lookups[key] = people.values_by_key('city').keys()
                    # lookups['country'] = ['ru', 'uk']
                    # lookups['city'] = ['ekb', 'msk', 'lon']
                for key in lookups:
                    query = self._dataset.all()
                    
                    query.find(key
            '''

            # XXX grouping vs. sorting???

            # sorting
            if self._order_by:
                ids = self._to_list()
                def _order_results():
                    # iterate over all possible distinct values for the key by which we sort
                    values = sorted(self._dataset.values_by_key(self._order_by))
                    if self._order_reversed:
                        values = reversed(values)
                    for value in values:
                        value_ids = self._dataset.ids_by_key_and_value(self._order_by, value)
                        #intersection = set(self._dataset._index[self._order_by][value]) & set(ids)
                        for i in value_ids:
                            if i in ids:
                                yield i
                                ids.pop(ids.index(i))   # distinct
                self._iter = iter(list(_order_results()))

class Dataset(object):
    """
    A query tool for list of dictionaries.
    Currently only 'flat' (i.e. not nested) dictionaries are supported.
    Data items are not immutable, but if you modify them, you have to rebuild indices manually.
    """
    def __init__(self, data):
        self.data = data
        self._rebuild_index()
        #self._rebuild_vectors()

    #---------------------+
    #  Proxies for Query  |
    #---------------------+

    def find(self, **kw):
        "Returns indices of all items having given keys (and values, if specified)."
        return Query(dataset=self, lookups=kw)

    def all(self):
        return self.find()

    def values(self, key):
        return self.all().values(key)

    #-------------------+
    #  Basic query API  |
    #-------------------+

    def doc_by_id(self, i):
        return self.data[i]

    def docs_by_ids(self, indices):
        return (self.data[index] for index in indices)

    def values_by_key(self, key):
        return self._index.get(key, {})

    def ids_by_key_and_value(self, key, value):
        return self.values_by_key(key).get(value, [])

    def ids_by_lookups(self, **kw):
        ids = None
        if not kw:
            return iter(xrange(0, len(self.data)))
        # intersecting subsets
        for key, value in kw.items():
            found = []
            if isinstance(value, exact):
                # exact match allowed, cut off a corner, grab dict keys and leave
                found.extend(self.ids_by_key_and_value(key, value._value))
            else:
                # gotta compare our value to each other
                for other_value in self.values_by_key(key):
                    if value == other_value:
                        # NOTE: get by the other value because ours can be a pseudo-value
                        found.extend(self.ids_by_key_and_value(key, other_value))
            if found:
                ids = ids.intersection(found) if ids else set(found)
        return [] if ids is None else iter(ids)

    #-------------------+
    #  Service methods  |
    #-------------------+

    def _rebuild_index(self):
        "Creates index for data. Representation: key -> value -> list_of_IDs."
        self._index = {}
        for i, item in enumerate(self.data):
            for key in item.keys():
                val = item[key]
                if val != None:
                    for v in self._unwrap_value(val):
                        self._index.setdefault(key, {}).setdefault(v, []).append(i)

    def _unwrap_value(self, value):
        "Wraps scalar in list; if value already was a list, it's returned intact."
        # (one level only. if there are more, will raise TypeError: unhashable type)
        if isinstance(value, list):
            return value
        else:
            return [value]
        #except TypeError:
        #    raise TypeError, 'could not index %s=%s' % (key, val)

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

if __name__ == '__main__':
    import doctest
    doctest.testmod()

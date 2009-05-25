# -*- coding: utf-8 -*-

from iterator_cached import CachedIterator
from aggregates import *

__doc__ = """
>>> from aggregates import Avg, Count
>>> import yaml
>>> data = yaml.load(open('people.yaml'))
>>> people = Dataset(data)
>>> people.inspect() == {'city': 6, 'name': 7, 'gender': 7, 'age': 6, 'job': 5, 'country': 7}
True
>>> list( people.find() )
[<Document 0>, <Document 1>, <Document 2>, <Document 3>, <Document 4>, <Document 5>, <Document 6>]
>>> list( people.find(country=any_) )
[<Document 0>, <Document 1>, <Document 2>, <Document 3>, <Document 4>, <Document 5>, <Document 6>]
>>> list( people.find(job=any_) )
[<Document 0>, <Document 1>, <Document 2>, <Document 3>, <Document 4>]
>>> list( people.find(country=not_(None)) )
[<Document 0>, <Document 1>, <Document 2>, <Document 3>, <Document 4>, <Document 5>, <Document 6>]
>>> list( people.find(country='ru') )
[<Document 0>, <Document 1>, <Document 2>]
>>> list( people.find(country=not_('ru')) )
[<Document 3>, <Document 4>, <Document 5>, <Document 6>]
>>> items = people.find(country='ru', gender=not_('m'))   # multiple conditions
>>> len(items)
1
>>> item = items[0]
>>> item
<Document 1>
>>> item._dict == None   # empty dict
True
>>> item.name
'Ksenya'
>>> item._dict == {'gender': 'f', 'age': 24, 'city': 'ekb', 'name': 'Ksenya', 'country': 'ru', 'job': 'teacher'}
True
>>> people.values('country')
['it', 'ru', 'rwanda']

# group by country
>>> for country in people.values('country'):
...     print country
it
ru
rwanda

# group by country; calculate average age
>>> for country in people.values('country'):
...     documents = people.find(country=country)
...     print country, str(Avg('age').count_for(documents))
it 22.6666666667
ru 26.6666666667
rwanda N/A

# group by country; count country population
>>> for country in sorted(people.values('country')):
...     documents = people.find(country=country)
...     print country, int(Count().count_for(documents))
it 3
ru 3
rwanda 1

# group by country and city
>>> for country in people.values('country'):
...     for city in people.find(country=country).values('city'):
...         print country, city
it firenze
ru ekb
ru spb
rwanda None

# group by country and city; count city population
>>> for country in people.values('country'):
...     for city in people.find(country=country).values('city'):
...         documents = people.find(country=country, city=city)
...         print country, city, int(Count().count_for(documents))
it firenze 3
ru ekb 2
ru spb 1
rwanda None 0

# group by country, city and gender
>>> for country in people.values('country'):
...     for city in people.find(country=country).values('city'):
...         for gender in people.find(country=country, city=city).values('gender'):
...             print country, city, gender
it firenze f
it firenze m
ru ekb f
ru ekb m
ru spb m

# group by country, city, and gender; calculate average age
>>> for country in people.values('country'):
...     for city in people.find(country=country).values('city'):
...         for gender in people.find(country=country, city=city).values('gender'):
...             documents = people.find(country=country, city=city, gender=gender)
...             print country, city, gender, str(Avg('age').count_for(documents))
it firenze f 16.5
it firenze m 35.0
ru ekb f 24.0
ru ekb m 25.0
ru spb m 31.0
>>> ##TODO: list(people.group_by('country'))
>>> ##TODO: list(people.group_by('country', 'city'))
>>> ##TODO: list(people.group_by('country', 'city', 'gender'))
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
    __hash__     = lambda self: hash(self.idx)
    __int__      = lambda self: self._idx
    __repr__     = lambda self: '<Document %d>' % self._idx
    def __getattr__(self, name):
        if name in self._fetch():
            return self._dict[name]
        raise AttributeError
    def _fetch(self):
        if not self._dict:
            self._dict = self._dataset.doc_by_id(self._idx)
            #for key, val in self._annotations.items():
            #    setattr(self, key, val)
            for key, val in self._dict.items():
                if not key.startswith('_'):
                    setattr(self, key, val)
        return self._dict

# DATASET AND QUERY

class Query(CachedIterator):
    """
    people.find(name='John').exclude(lastname='Connor').values('country').annotate(Avg('age'))
    
          people.find(name='john').find(last_name='connor').exclude(location='new york')
                |                 |                       |
     __dataset__|__query__________|__query________________|__query____________________

     Currently there's only find() method because any exclude(foo=123) is just find(foo=not_(123)).

    TODO: exact lookups.
          They allow direct lookup via index (very fast!) and do not require iterating
          over _all_ existing values and comparing each with lookup value.
    TODO: multi-value lookups ("name=['john','mary']" -- compare list or items in list? maybe "name=in_(['john','mary')"?)
    TODO: date lookups (year, month, day, time, range, cmp)
    """
    def init(self, dataset, lookups={}, order_by={}, order_reversed=False, **kw):
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
        "Returns a new Query instance with existing + given lookups."
        return self._clone(extra_lookups=kw)

    def order_by(self, key):
        if key.startswith('-'):
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
        for i, item in enumerate(self.data):
            item = self.data[i]
            try:
                for key, value in kw.items():
                    # item mush have each of the keys specified
                    if not key in item:
                        raise ItemDoesNotMatch
                    # warning: cmp order matters because value can be a SpecialValue instance
                    # with overloaded comparison method
                    if value != item[key]:
                        raise ItemDoesNotMatch
            except ItemDoesNotMatch:
                continue
            else:
                yield i

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

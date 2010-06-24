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

__all__ = ['Document']

class Document(object):
    """
    A basic wrapper for a record in a collection
    (i.e. a dictionary with numeric identifier).

    Usage::

        >>> from dark.document import Document
        >>> d = Document(123, {'foo': 'bar'})
        >>> d
        <Document 123>
        >>> d.foo
        'bar'
        >>> d.quux
        Traceback (most recent call last):
            ...
        AttributeError
        >>> d.get('foo', None)
        'bar'
        >>> d.items()
        [('foo', 'bar')]
        >>> d.keys()
        ['foo']
        >>> d.values()
        ['bar']
        >>> 'foo' in d
        True
        >>> 'quux' in d
        False
        >>> d['foo']
        'bar'
        >>> int(d)
        123

    Comparison of documents:

        >>> d2 = Document(123, {})  # primary key matters, data doesn't
        >>> d3 = Document(456, {})
        >>> d == d2
        True
        >>> d == d3
        False

    """
    # XXX Document should know nothing about storage implementation.
    #     its constructor should recieve a decorated function which
    #     contains either links to storage instance etc. or the data,
    #     and returns the dictionary.
    #     i.e. Document._fetch should be an external function, and
    #     Document._idx should be contained within that function
    #     _or_ a primary key which indeed does not depend on implementation.
    #     Question: what about __int__, __repr__, etc.?
    #def __init__(self, storage, pk, data=None):   #, annotations):
    def __init__(self, pk, data):   #, annotations):
        #assert isinstance(storage, BaseCollection), 'BaseCollection instance expected, got %s' % storage
        assert isinstance(pk, int), 'integer expected, got %s' % pk
        #self._storage = storage
        self._pk = pk
        self._data = data
        self._attrs_assigned = False
        #self._annotations = annotations
        self.init()

    def init(self):
        pass

    # dictionary methods
    def get(self, k, v=None): return self._data.get(k,v)
    def items(self):    return self._data.items()
    def keys(self):     return self._data.keys()
    def values(self):   return self._data.values()
    def __contains__(self, key): return key in self._data
    def __getitem__(self, key): return self._data[key]
    # special convenience magic
    def __hash__(self): return hash(self._pk)
    def __int__(self):  return self._pk
    def __repr__(self): return '<Document %d>' % self._pk
    def __eq__(self, other):
        return isinstance(other, Document) and other._pk == self._pk
    def __getattr__(self, name):
        "Makes first-level dictionary keys available as instance attributes."
        if not self._attrs_assigned:
            for key, val in self._data.items():
                if not key[0] == '_':
                    setattr(self, key, val)
            self._attrs_assigned = True
        if name in self._data:
            return self._data[name]
        raise AttributeError
#    @property
#    def _dict(self):
#        "Dynamically fetches document data from storage by primary key."
#        if not self._data:
#            self._data = self._storage.fetch_one(self._pk)
#            #for key, val in self._annotations.items():
#            #    setattr(self, key, val)
#        return self._data

if __name__ == '__main__':
    import doctest
    doctest.testmod()
